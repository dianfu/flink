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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link DataStreamBeamPythonFunctionRunner} is responsible for starting a beam python harness to
 * execute user defined python function.
 */
@Internal
public class DataStreamBeamPythonFunctionRunner extends BeamPythonFunctionRunner {

    private static final String TRANSFORM_ID_PREFIX = "transform-";
    private static final String COLLECTION_PREFIX = "collection-";

    private final TypeInformation<?> inputType;
    private final TypeInformation<?> outputType;
    private final String functionUrn;
    private final List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions;

    private DataStreamBeamPythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            TypeInformation<?> inputType,
            TypeInformation<?> outputType,
            String functionUrn,
            List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend<?> stateBackend,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderParam.DataType inputDataType,
            FlinkFnApi.CoderParam.DataType outputDataType,
            FlinkFnApi.CoderParam.OutputMode outputMode) {
        super(
                taskName,
                environmentManager,
                jobOptions,
                flinkMetricContainer,
                stateBackend,
                keySerializer,
                namespaceSerializer,
                memoryManager,
                managedMemoryFraction,
                inputDataType,
                outputDataType,
                outputMode);
        this.inputType = inputType;
        this.outputType = outputType;
        this.functionUrn = functionUrn;
        this.userDefinedDataStreamFunctions =
                Preconditions.checkNotNull(userDefinedDataStreamFunctions);

        // reverse the list since the last one is the head operator of the operation tree
        Collections.reverse(this.userDefinedDataStreamFunctions);
    }

    public static DataStreamBeamPythonFunctionRunner of(
            String taskName,
            PythonEnvironmentManager environmentManager,
            TypeInformation<?> inputType,
            TypeInformation<?> outputType,
            String functionUrn,
            List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend<?> stateBackend,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderParam.DataType inputDataType,
            FlinkFnApi.CoderParam.DataType outputDataType,
            FlinkFnApi.CoderParam.OutputMode outputMode) {
        return new DataStreamBeamPythonFunctionRunner(
                taskName,
                environmentManager,
                inputType,
                outputType,
                functionUrn,
                userDefinedDataStreamFunctions,
                jobOptions,
                flinkMetricContainer,
                stateBackend,
                keySerializer,
                namespaceSerializer,
                memoryManager,
                managedMemoryFraction,
                inputDataType,
                outputDataType,
                outputMode);
    }

    @Override
    protected RunnerApi.Coder getInputCoderProto() {
        return getInputOutputCoderProto(inputType, inputDataType);
    }

    @Override
    protected RunnerApi.Coder getOutputCoderProto() {
        return getInputOutputCoderProto(outputType, outputDataType);
    }

    private RunnerApi.Coder getInputOutputCoderProto(
            TypeInformation<?> typeInformation, FlinkFnApi.CoderParam.DataType dataType) {
        FlinkFnApi.CoderParam.Builder coderParamBuilder = FlinkFnApi.CoderParam.newBuilder();
        FlinkFnApi.TypeInfo typeinfo =
                PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(typeInformation);
        coderParamBuilder.setTypeInfo(typeinfo);
        coderParamBuilder.setDataType(dataType);
        coderParamBuilder.setOutputMode(outputMode);
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(FLINK_CODER_URN)
                                .setPayload(
                                        org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf
                                                .ByteString.copyFrom(
                                                coderParamBuilder.build().toByteArray()))
                                .build())
                .build();
    }

    @Override
    protected Map<String, RunnerApi.PTransform> getTransforms() {
        Map<String, RunnerApi.PTransform> transforms = new HashMap<>();
        for (int i = 0; i < userDefinedDataStreamFunctions.size(); i++) {
            RunnerApi.PTransform.Builder transformBuilder =
                    RunnerApi.PTransform.newBuilder()
                            .setUniqueName(TRANSFORM_ID_PREFIX + i)
                            .setSpec(
                                    RunnerApi.FunctionSpec.newBuilder()
                                            .setUrn(functionUrn)
                                            .setPayload(
                                                    org.apache.beam.vendor.grpc.v1p26p0.com.google
                                                            .protobuf.ByteString.copyFrom(
                                                            userDefinedDataStreamFunctions
                                                                    .get(i)
                                                                    .toByteArray()))
                                            .build());

            // build inputs
            if (i == 0) {
                transformBuilder.putInputs(MAIN_INPUT_NAME, INPUT_ID);
            } else {
                transformBuilder.putInputs(MAIN_INPUT_NAME, COLLECTION_PREFIX + (i - 1));
            }

            // build outputs
            if (i == userDefinedDataStreamFunctions.size() - 1) {
                transformBuilder.putOutputs(MAIN_OUTPUT_NAME, OUTPUT_ID);
            } else {
                transformBuilder.putOutputs(MAIN_OUTPUT_NAME, COLLECTION_PREFIX + i);
            }
        }
        return transforms;
    }
}
