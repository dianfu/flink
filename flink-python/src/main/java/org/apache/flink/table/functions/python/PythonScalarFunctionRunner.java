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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.AbstractPythonFunctionRunner;
import org.apache.flink.python.PythonFunctionExecutionResultReceiver;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.python.PythonFunctionInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.sdk.coders.Coder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 */
@Internal
public class PythonScalarFunctionRunner extends AbstractPythonFunctionRunner<Row, Row> {

	private static final String SCHMEA_CODER_URN = "flink:coder:schema:v1";
	private static final String SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1";

	private static final String INPUT_ID = "input";
	private static final String OUTPUT_ID = "output";
	private static final String TRANSFORM_ID = "transform";

	private static final String MAIN_INPUT_NAME = "input";
	private static final String MAIN_OUTPUT_NAME = "input";

	private static final String INPUT_CODER_ID = "input_coder";
	private static final String OUTPUT_CODER_ID = "output_coder";
	private static final String WINDOW_CODER_ID = "window_coder";

	private static final String WINDOW_STRATEGY = "windowing-strategy";

	private final List<PythonFunctionInfo> scalarFunctions;

	public PythonScalarFunctionRunner(
		String taskName,
		PythonFunctionExecutionResultReceiver<Row> resultReceiver,
		List<PythonFunctionInfo> scalarFunctions,
		Map<String, String> pythonEnv) {
		super(taskName, resultReceiver, pythonEnv);
		this.scalarFunctions = Objects.requireNonNull(scalarFunctions);
	}

	@Override
	@SuppressWarnings("unchecked")
	public ExecutableStage createExecutableStage() {
		RunnerApi.Components components =
			RunnerApi.Components.newBuilder()
				.putPcollections(
					INPUT_ID,
					RunnerApi.PCollection.newBuilder()
						.setWindowingStrategyId(WINDOW_STRATEGY)
						.setCoderId(INPUT_CODER_ID)
						.build())
				.putPcollections(
					OUTPUT_ID,
					RunnerApi.PCollection.newBuilder()
						.setWindowingStrategyId(WINDOW_STRATEGY)
						.setCoderId(OUTPUT_CODER_ID)
						.build())
				.putTransforms(
					TRANSFORM_ID,
					RunnerApi.PTransform.newBuilder()
						.setUniqueName(TRANSFORM_ID)
						.setSpec(RunnerApi.FunctionSpec.newBuilder()
									.setUrn(SCALAR_FUNCTION_URN)
									.setPayload(
										org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString.copyFrom(
											getUserDefinedFunctionsProto().toByteArray()))
									.build())
						.putInputs(MAIN_INPUT_NAME, INPUT_ID)
						.putOutputs(MAIN_OUTPUT_NAME, OUTPUT_ID)
						.build())
				.putWindowingStrategies(
					WINDOW_STRATEGY,
					RunnerApi.WindowingStrategy.newBuilder()
						.setWindowCoderId(WINDOW_CODER_ID)
						.build())
				.putCoders(
					INPUT_CODER_ID,
					getInputCoderProto())
				.putCoders(
					OUTPUT_CODER_ID,
					getOutputCoderProto())
				.putCoders(
					WINDOW_CODER_ID,
					getWindowCoderProto())
				.build();

		PipelineNode.PCollectionNode input =
			PipelineNode.pCollection(INPUT_ID, components.getPcollectionsOrThrow(INPUT_ID));
		List<SideInputReference> sideInputs = Collections.EMPTY_LIST;
		List<UserStateReference> userStates = Collections.EMPTY_LIST;
		List<TimerReference> timers = Collections.EMPTY_LIST;
		List<PipelineNode.PTransformNode> transforms =
			Collections.singletonList(
				PipelineNode.pTransform(TRANSFORM_ID, components.getTransformsOrThrow(TRANSFORM_ID)));
		List<PipelineNode.PCollectionNode> outputs =
			Collections.singletonList(
				PipelineNode.pCollection(OUTPUT_ID, components.getPcollectionsOrThrow(OUTPUT_ID)));
		return ImmutableExecutableStage.of(
			components, getEnvironment(), input, sideInputs, userStates, timers, transforms, outputs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Coder<Row> getInputCoder() {
		return (Coder<Row>) BeamTypeUtils.toCoder(getInputType());
	}

	@Override
	@SuppressWarnings("unchecked")
	public Coder<Row> getOutputCoder() {
		return (Coder<Row>) BeamTypeUtils.toCoder(getOutputType());
	}

	private RowType getInputType() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			List<Object> inputs = pythonFunctionInfo.getInputs();
			List<DataType> inputTypes = pythonFunctionInfo.getInputTypes();
			for (int i = 0; i < inputs.size(); i++) {
				Object input = inputs.get(i);
				if (input instanceof PythonFunctionInfo) {
					// TODO support chained Python scalar functions
					throw new UnsupportedOperationException();
				} else {
					rowFields.add(new RowType.RowField(
						"f" + rowFields.size(),
						inputTypes.get(i).getLogicalType()));
				}
			}
		}

		return new RowType(rowFields);
	}

	private RowType getOutputType() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			rowFields.add(new RowType.RowField(
				"f" + rowFields.size(),
				pythonFunctionInfo.getResultType().getLogicalType()));
		}

		return new RowType(rowFields);
	}

	private FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			builder.addUdfs(getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		return builder.build();
	}

	private FlinkFnApi.UserDefinedFunction getUserDefinedFunctionProto(PythonFunctionInfo pythonFunctionInfo) {
		FlinkFnApi.UserDefinedFunction.Builder builder = FlinkFnApi.UserDefinedFunction.newBuilder();
		builder.setPayload(ByteString.copyFrom(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		for (Object input : pythonFunctionInfo.getInputs()) {
			FlinkFnApi.UserDefinedFunction.Input.Builder inputProto =
				FlinkFnApi.UserDefinedFunction.Input.newBuilder();
			if (input instanceof PythonFunctionInfo) {
				inputProto.setUdf(getUserDefinedFunctionProto((PythonFunctionInfo) input));
			} else {
				inputProto.setInputOffset((Integer) input);
			}
			builder.addInputs(inputProto);
		}
		return builder.build();
	}

	private RunnerApi.Coder getInputCoderProto() {
		return getRowCoderProto(getInputType());
	}

	private RunnerApi.Coder getOutputCoderProto() {
		return getRowCoderProto(getOutputType());
	}

	private RunnerApi.Coder getRowCoderProto(RowType rowType) {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(SCHMEA_CODER_URN)
					.setPayload(org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString.copyFrom(
						BeamTypeUtils.toProtoType(rowType).getRowSchema().toByteArray()))
					.build())
			.build();
	}

	private RunnerApi.Coder getWindowCoderProto() {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
					.build())
			.build();
	}
}
