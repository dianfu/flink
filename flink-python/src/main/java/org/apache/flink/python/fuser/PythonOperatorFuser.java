/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.fuser;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.python.util.PythonConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.python.PythonKeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.compress.utils.Lists;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory class which attempts to fuse all available operators which contain Python functions.
 *
 * <p>An operator could be fused to it's predecessor if all of the following conditions are met:
 *
 * <ul>
 *   <li>Both of them are Python operators
 *   <li>The parallelism, the maximum parallelism and the slot sharing group are all the same
 *   <li>The chaining strategy is ChainingStrategy.ALWAYS and the chaining strategy of the
 *       predecessor isn't ChainingStrategy.NEVER
 *   <li>This partitioner between them is ForwardPartitioner
 * </ul>
 *
 * The properties of the generated fused operator are as following:
 *
 * <p><url>
 * <li>The name is the concatenation of all the names of the fused operators
 * <li>The parallelism, the maximum parallelism and the slot sharing group are from one of the fused
 *     operators as all of them are the same between the fused operators
 * <li>The chaining strategy is the same as the head operator
 * <li>The uid and the uidHash are the same as the head operator </url>
 */
public class PythonOperatorFuser {

    private final List<Transformation<?>> transformations;

    private final ExecutionConfig executionConfig;

    private final boolean isChainingEnabled;

    // Keep track of which transforms we have already visited
    private final Set<Transformation<?>> visited = new HashSet<>();

    // Key-value pairs where the value is the outputs of the key
    private final transient Map<Transformation<?>, Set<Transformation<?>>> outputMap =
            new HashMap<>();

    private PythonOperatorFuser(
            List<Transformation<?>> transformations,
            ExecutionConfig executionConfig,
            boolean isChainingEnabled) {
        this.transformations = checkNotNull(transformations);
        this.executionConfig = checkNotNull(executionConfig);
        this.isChainingEnabled = isChainingEnabled;
        buildOutputMap();
    }

    private void buildOutputMap() {
        for (Transformation<?> transformation : transformations) {
            for (Transformation<?> input : transformation.getInputs()) {
                Set<Transformation<?>> outputs =
                        outputMap.computeIfAbsent(input, i -> Sets.newHashSet());
                outputs.add(transformation);
            }
        }
    }

    private List<Transformation<?>> fuse() throws Exception {
        List<Transformation<?>> fusedTransformations = Lists.newArrayList();
        for (Transformation<?> transformation : transformations) {
            FuseInfo fused = fuse(transformation);
            fusedTransformations.add(fused.newTransformation);
            if (fused.oldTransformations != null) {
                for (Transformation<?> oldTrans : fused.oldTransformations) {
                    fusedTransformations.remove(oldTrans);
                }
            }
        }

        return fusedTransformations;
    }

    private FuseInfo fuse(Transformation<?> transform) throws Exception {
        if (visited.contains(transform)) {
            return FuseInfo.of(transform);
        }

        if (transform.getMaxParallelism() <= 0) {
            // if the max parallelism hasn't been set, set it to the job-wise max parallelism
            // from the ExecutionConfig.
            int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
            if (globalMaxParallelismFromConfig > 0) {
                transform.setMaxParallelism(globalMaxParallelismFromConfig);
            }
        }

        FuseInfo fuseInfo = null;
        if (PythonConfigUtil.isPythonOperator(transform)) {
            if (transform instanceof OneInputTransformation) {
                Transformation<?> input = transform.getInputs().get(0);
                if (!PythonConfigUtil.isPythonOperator(input)) {
                    if (input instanceof PartitionTransformation
                            && ((PartitionTransformation<?>) input).getPartitioner()
                                    instanceof ForwardPartitioner) {
                        input = input.getInputs().get(0);
                    } else {
                        visited.add(transform);
                        return FuseInfo.of(transform);
                    }
                }

                if (isChainable(input, transform)) {
                    Transformation<?> fused = fuse(input, transform);
                    for (Transformation<?> output : outputMap.get(transform)) {
                        replaceInput(output, transform, fused);
                    }
                    fuseInfo = FuseInfo.of(fused, Arrays.asList(input, transform));
                }
            }
        }

        if (fuseInfo == null) {
            fuseInfo = FuseInfo.of(transform);
        }

        visited.add(transform);
        return fuseInfo;
    }

    private OneInputTransformation<?, ?> fuse(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        SimpleOperatorFactory<?> upOperatorFactory =
                (SimpleOperatorFactory<?>) getOperatorFactory(upTransform);
        SimpleOperatorFactory<?> downOperatorFactory =
                (SimpleOperatorFactory<?>) getOperatorFactory(downTransform);
        PythonKeyedProcessOperator<?> upStreamOperator =
                (PythonKeyedProcessOperator<?>) upOperatorFactory.getOperator();
        PythonKeyedProcessOperator<?> downStreamOperator =
                (PythonKeyedProcessOperator<?>) downOperatorFactory.getOperator();

        DataStreamPythonFunctionInfo upPythonFunctionInfo =
                upStreamOperator.getPythonFunctionInfo();
        DataStreamPythonFunctionInfo downPythonFunctionInfo =
                downStreamOperator.getPythonFunctionInfo();
        Preconditions.checkArgument(downPythonFunctionInfo.getInputs().length == 0);
        DataStreamPythonFunctionInfo pythonFunctionInfo =
                new DataStreamPythonFunctionInfo(
                        downPythonFunctionInfo.getPythonFunction(),
                        upPythonFunctionInfo,
                        downPythonFunctionInfo.getFunctionType());

        OneInputStreamOperator<?, ?> fusedOperator =
                new PythonKeyedProcessOperator<>(
                        upStreamOperator.getConfig().getConfig(),
                        upStreamOperator.getInputType(),
                        downStreamOperator.getProducedType(),
                        pythonFunctionInfo);

        return new OneInputTransformation(
                upTransform.getInputs().get(0),
                upTransform.getName() + ", " + downTransform.getName(),
                fusedOperator,
                downTransform.getOutputType(),
                upTransform.getParallelism());
    }

    private boolean isChainable(Transformation<?> upTransform, Transformation<?> downTransform) {
        return upTransform.getParallelism() == downTransform.getParallelism()
                && upTransform.getMaxParallelism() == downTransform.getMaxParallelism()
                && isSameSlotSharingGroup(upTransform, downTransform)
                && areOperatorsChainable(upTransform, downTransform)
                && outputMap.get(upTransform).size() == 1
                && isChainingEnabled;
    }

    // ----------------------- Utility Methods -----------------------

    public static void fuse(StreamExecutionEnvironment env) throws Exception {
        Field transformationsField =
                StreamExecutionEnvironment.class.getDeclaredField("transformations");
        transformationsField.setAccessible(true);
        List<Transformation<?>> transformations =
                (List<Transformation<?>>) transformationsField.get(env);
        PythonOperatorFuser fuser =
                new PythonOperatorFuser(transformations, env.getConfig(), env.isChainingEnabled());
        List<Transformation<?>> fusedTransformations = fuser.fuse();
        transformationsField.set(env, fusedTransformations);
    }

    private static boolean isSameSlotSharingGroup(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        return (upTransform.getSlotSharingGroup() == null
                        && downTransform.getSlotSharingGroup() == null)
                || (upTransform.getSlotSharingGroup() != null
                        && upTransform
                                .getSlotSharingGroup()
                                .equals(downTransform.getSlotSharingGroup()));
    }

    private static boolean areOperatorsChainable(
            Transformation<?> upTransform, Transformation<?> downTransform) {
        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        StreamOperatorFactory<?> upStreamOperator = getOperatorFactory(upTransform);
        StreamOperatorFactory<?> downStreamOperator = getOperatorFactory(downTransform);

        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        return isChainable;
    }

    private static StreamOperatorFactory<?> getOperatorFactory(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return ((OneInputTransformation<?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof TwoInputTransformation) {
            return ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory();
        } else if (transform instanceof AbstractMultipleInputTransformation) {
            return ((AbstractMultipleInputTransformation<?>) transform).getOperatorFactory();
        } else {
            return null;
        }
    }

    private static void replaceInput(
            Transformation<?> transformation,
            Transformation<?> oldInput,
            Transformation<?> newInput)
            throws Exception {
        if (transformation instanceof OneInputTransformation
                || transformation instanceof FeedbackTransformation
                || transformation instanceof SideOutputTransformation
                || transformation instanceof ReduceTransformation
                || transformation instanceof SinkTransformation
                || transformation instanceof LegacySinkTransformation
                || transformation instanceof TimestampsAndWatermarksTransformation) {
            Field inputField = transformation.getClass().getDeclaredField("input");
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else if (transformation instanceof TwoInputTransformation) {
            Field inputField;
            if (((TwoInputTransformation<?, ?, ?>) transformation).getInput1() == oldInput) {
                inputField = transformation.getClass().getDeclaredField("input1");
            } else {
                inputField = transformation.getClass().getDeclaredField("input2");
            }
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else if (transformation instanceof UnionTransformation
                || transformation instanceof AbstractMultipleInputTransformation) {
            Field inputsField = transformation.getClass().getDeclaredField("inputs");
            inputsField.setAccessible(true);
            List<Transformation<?>> newInputs = Lists.newArrayList();
            newInputs.addAll(transformation.getInputs());
            newInputs.remove(oldInput);
            newInputs.add(newInput);
            inputsField.set(transformation, newInputs);
        } else if (transformation instanceof AbstractBroadcastStateTransformation) {
            Field inputField;
            if (((AbstractBroadcastStateTransformation<?, ?, ?>) transformation).getRegularInput()
                    == oldInput) {
                inputField = transformation.getClass().getDeclaredField("regularInput");
            } else {
                inputField = transformation.getClass().getDeclaredField("broadcastInput");
            }
            inputField.setAccessible(true);
            inputField.set(transformation, newInput);
        } else {
            throw new RuntimeException("Unsupported transformation: " + transformation);
        }
    }

    // ----------------------- Utility Classes -----------------------

    private static class FuseInfo {
        /** The transformation which represents the chaining of the {@link #oldTransformations}. */
        public final Transformation<?> newTransformation;

        /** The transformations which will be chained together. */
        public final Collection<Transformation<?>> oldTransformations;

        private FuseInfo(
                Transformation<?> newTransformation,
                Collection<Transformation<?>> oldTransformations) {
            this.newTransformation = newTransformation;
            this.oldTransformations = oldTransformations;
        }

        /** No chaining happens. */
        public static FuseInfo of(Transformation<?> newTransformation) {
            return new FuseInfo(newTransformation, null);
        }

        public static FuseInfo of(
                Transformation<?> newTransformation,
                Collection<Transformation<?>> oldTransformations) {
            return new FuseInfo(newTransformation, oldTransformations);
        }
    }
}
