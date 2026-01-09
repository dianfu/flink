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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for exec Python Async Calc.
 *
 * <p>This class handles Python async scalar functions which require asynchronous execution. Unlike
 * {@link CommonExecPythonCalc}, this creates dedicated async operators for Python async functions.
 */
public abstract class CommonExecPythonAsyncCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String PYTHON_ASYNC_CALC_TRANSFORMATION = "python-async-calc";

    public static final String FIELD_NAME_PROJECTION = "projection";

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    public CommonExecPythonAsyncCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());

        OneInputTransformation<RowData, RowData> ret =
                createPythonAsyncOneInputTransformation(
                        inputTransform,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        pythonConfig);

        // Note: Python async operators will manage their own async execution,
        // so we don't need to declare managed memory usage here
        return ret;
    }

    /**
     * Creates the transformation for Python async scalar functions.
     *
     * <p>This method should be implemented by subclasses to create the appropriate async operator
     * for executing Python async scalar functions.
     *
     * @param inputTransform The input transformation
     * @param config The exec node configuration
     * @param classLoader The class loader
     * @param pythonConfig The Python configuration
     * @return The output transformation with Python async operator
     */
    protected abstract OneInputTransformation<RowData, RowData>
            createPythonAsyncOneInputTransformation(
                    Transformation<RowData> inputTransform,
                    ExecNodeConfig config,
                    ClassLoader classLoader,
                    Configuration pythonConfig);

    /**
     * Gets the async operator for executing Python async scalar functions.
     *
     * <p>Subclasses should implement this to provide the specific operator implementation for
     * Python async execution.
     *
     * @param config The exec node configuration
     * @param classLoader The class loader
     * @param pythonConfig The Python configuration
     * @param inputTypeInfo The input type information
     * @param outputTypeInfo The output type information
     * @return The Python async operator
     */
    protected abstract OneInputStreamOperator<RowData, RowData> getPythonAsyncOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            InternalTypeInfo<RowData> inputTypeInfo,
            InternalTypeInfo<RowData> outputTypeInfo);

    protected List<RexNode> getProjection() {
        return projection;
    }
}
