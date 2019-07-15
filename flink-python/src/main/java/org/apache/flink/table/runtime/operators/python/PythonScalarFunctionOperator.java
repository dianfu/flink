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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionExecutionResultReceiver;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The {@link PythonScalarFunctionOperator} is responsible for executing Python {@link ScalarFunction}s.
 * The Python {@link ScalarFunction}s are executed in separate Python execution environment.
 *
 * <p>The inputs are assumed as the following format:
 * {{{
 *   +------------------+------------------------+
 *   | forwarded fields | scalar function inputs |
 *   +------------------+------------------------+
 * }}}.
 *
 * <p>The outputs will be as the following format:
 * {{{
 *   +------------------+-------------------------+
 *   | forwarded fields | scalar function results |
 *   +------------------+-------------------------+
 * }}}.
 */
@Internal
public class PythonScalarFunctionOperator extends AbstractPythonFunctionOperator<CRow, CRow> {

	private static final long serialVersionUID = 1L;

	/**
	 * The Python {@link ScalarFunction}s to be executed.
	 */
	private final List<PythonFunctionInfo> scalarFunctions;

	/**
	 * The number of forwarded fields in the input element.
	 */
	private final int forwardedFieldCnt;

	private transient StreamRecordCRowWrappingCollector cRowWrapper;
	private transient LinkedBlockingQueue<CRow> inputQueue;
	private transient LinkedBlockingQueue<Row> udfResultQueue;

	public PythonScalarFunctionOperator(
		List<PythonFunctionInfo> scalarFunctions,
		int forwardedFieldCnt) {
		super();
		this.scalarFunctions = Objects.requireNonNull(scalarFunctions);
		this.forwardedFieldCnt = forwardedFieldCnt;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);
		this.inputQueue = new LinkedBlockingQueue<>();
		this.udfResultQueue = new LinkedBlockingQueue<>();
	}

	@Override
	public void processElement(StreamRecord<CRow> element) throws Exception {
		inputQueue.add(element.getValue());
		super.processElement(element);
		emitResults();
	}

	@Override
	public PythonFunctionRunner<CRow> createPythonFunctionRunner() {
		PythonFunctionExecutionResultReceiver<Row> udfResultReceiver = input -> {
			// handover to queue, do not block the grpc thread
			udfResultQueue.put(input);
		};

		return new PythonScalarFunctionRunner(
			new org.apache.flink.table.functions.python.PythonScalarFunctionRunner(
				getRuntimeContext().getTaskName(),
				udfResultReceiver,
				scalarFunctions,
				scalarFunctions.get(0).getPythonFunction().getPythonEnv()));
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() {
		Row udfResult;
		while ((udfResult = udfResultQueue.poll()) != null) {
			CRow input = inputQueue.poll();
			cRowWrapper.setChange(input.change());
			Row joinedRow = Row.join(getForwardedRow(input.row()), udfResult);
			cRowWrapper.collect(joinedRow);
		}
	}

	private Row getForwardedRow(Row input) {
		int[] inputs = new int[forwardedFieldCnt];
		for (int i = 0; i < inputs.length; i++) {
			inputs[i] = i;
		}
		return Row.project(input, inputs);
	}

	private class PythonScalarFunctionRunner implements PythonFunctionRunner<CRow> {

		private final PythonFunctionRunner<Row> pythonFunctionRunner;

		PythonScalarFunctionRunner(PythonFunctionRunner<Row> pythonFunctionRunner) {
			this.pythonFunctionRunner = pythonFunctionRunner;
		}

		@Override
		public void open() throws Exception {
			pythonFunctionRunner.open();
		}

		@Override
		public void close() throws Exception {
			pythonFunctionRunner.close();
		}

		@Override
		public void startBundle() throws Exception {
			pythonFunctionRunner.startBundle();
		}

		@Override
		public void finishBundle() throws Exception {
			pythonFunctionRunner.finishBundle();
		}

		@Override
		public void processElement(CRow element) {
			pythonFunctionRunner.processElement(getPythonUdfInputRow(element.row()));
		}

		private Row getPythonUdfInputRow(Row input) {
			int[] inputs = new int[input.getArity() - forwardedFieldCnt];
			for (int i = 0; i < inputs.length; i++) {
				inputs[i] = forwardedFieldCnt + i;
			}
			return Row.project(input, inputs);
		}
	}
}
