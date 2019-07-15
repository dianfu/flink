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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * An base class for {@link PythonFunctionRunner}.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonFunctionRunner<IN, OUT> implements PythonFunctionRunner<IN> {

	private static final String INPUT_ID = "input";

	/**
	 * The default number of workers concurrently running on one node.
	 */
	private static final int DEFAULT_SDK_WORKER_PARALLELISM = 1;

	private final String taskName;
	private final PythonFunctionExecutionResultReceiver<OUT> resultReceiver;

	private transient RunnerApi.Environment environment;
	private transient PythonFunctionRunnerContext context;
	private transient StageBundleFactory stageBundleFactory;
	private transient StateRequestHandler stateRequestHandler;
	private transient BundleProgressHandler progressHandler;

	private transient RemoteBundle remoteBundle;
	private transient FnDataReceiver<WindowedValue<?>> mainInputReceiver;

	private transient Coder<IN> inputCoder;
	private transient Coder<OUT> outputCoder;

	private transient ByteArrayInputStreamWithPos bais;
	private transient ByteArrayOutputStreamWithPos baos;

	public AbstractPythonFunctionRunner(
		String taskName,
		PythonFunctionExecutionResultReceiver<OUT> resultReceiver,
		Map<String, String> pythonEnv) {
		this.taskName = Objects.requireNonNull(taskName);
		this.resultReceiver = Objects.requireNonNull(resultReceiver);
		this.environment = createPythonProcessExecutionEnvironment(pythonEnv);
	}

	@Override
	public void open() throws Exception {
		this.bais = new ByteArrayInputStreamWithPos();
		this.baos = new ByteArrayOutputStreamWithPos();
		this.inputCoder = getInputCoder();
		this.outputCoder = getOutputCoder();

		ExecutableStage executableStage = createExecutableStage();

		PortablePipelineOptions portableOptions =
			PipelineOptionsFactory.as(PortablePipelineOptions.class);
		// TODO: Make the SDKWorker parallelism configurable
		portableOptions.setSdkWorkerParallelism(DEFAULT_SDK_WORKER_PARALLELISM);
		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		this.context = DefaultPythonFunctionRunnerContext.create(
			JobInfo.create(taskName, taskName, generateEmptyRetrievalToken(), pipelineOptions));

		stageBundleFactory = context.getStageBundleFactory(executableStage);
		stateRequestHandler = getStateRequestHandler();
		progressHandler = getProgressHandler();
	}

	@Override
	public void close() throws Exception {
		if (context != null) {
			context.close();
			context = null;
		}
	}

	@Override
	public void startBundle() {
		OutputReceiverFactory receiverFactory =
			new OutputReceiverFactory() {

				// the input value type is always byte array
				@SuppressWarnings("unchecked")
				@Override
				public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
					return input -> {
						bais.setBuffer(input.getValue(), 0, input.getValue().length);
						resultReceiver.accept(outputCoder.decode(bais));
					};
				}
			};

		try {
			remoteBundle = stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
			mainInputReceiver =
				Objects.requireNonNull(
					remoteBundle.getInputReceivers().get(INPUT_ID),
					"Failed to retrieve main input receiver.");
		} catch (Exception e) {
			throw new RuntimeException("Failed to start remote bundle", e);
		}
	}

	@Override
	public void finishBundle() {
		try {
			remoteBundle.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to close remote bundle", e);
		}
	}

	@Override
	public void processElement(IN element) {
		try {
			baos.reset();
			inputCoder.encode(element, baos);
			mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(baos.getBuf()));
		} catch (Exception e) {
			throw new RuntimeException("Failed to process element when sending data to Python SDK harness.", e);
		}
	}

	public RunnerApi.Environment getEnvironment() {
		return this.environment;
	}

	public abstract ExecutableStage createExecutableStage();

	public abstract Coder<IN> getInputCoder();

	public abstract Coder<OUT> getOutputCoder();

	private StateRequestHandler getStateRequestHandler() {
		return new StateRequestHandlerImpl();
	}

	private static class StateRequestHandlerImpl implements StateRequestHandler {

		@Override
		public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request) {
			return null;
		}
	}

	private BundleProgressHandler getProgressHandler() {
		return new BundleProgressHandlerImpl();
	}

	private static class BundleProgressHandlerImpl implements BundleProgressHandler {
		@Override
		public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {}

		@Override
		public void onCompleted(BeamFnApi.ProcessBundleResponse response) {}
	}

	private RunnerApi.Environment createPythonProcessExecutionEnvironment(Map<String, String> pythonEnv) {
		Objects.requireNonNull(pythonEnv.get("python"));
		Objects.requireNonNull(pythonEnv.get("pip"));
		Objects.requireNonNull(pythonEnv.get("command"));

		return Environments.createProcessEnvironment(
			pythonEnv.getOrDefault("os", ""),
			pythonEnv.getOrDefault("arch", ""),
			pythonEnv.get("command"),
			pythonEnv);
	}

	private String generateEmptyRetrievalToken() throws Exception {
		final File retrievalToken = Files.createTempFile("retrieval_token", "json").toFile();
		final FileOutputStream fos = new FileOutputStream(retrievalToken);
		final DataOutputStream dos = new DataOutputStream(fos);
		dos.writeBytes("{\"manifest\": {}}");
		dos.flush();
		dos.close();
		return retrievalToken.getAbsolutePath();
	}
}
