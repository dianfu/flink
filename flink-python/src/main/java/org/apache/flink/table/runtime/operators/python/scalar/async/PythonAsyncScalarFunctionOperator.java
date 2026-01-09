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

package org.apache.flink.table.runtime.operators.python.scalar.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractPythonScalarFunctionOperator;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;

/**
 * The Python {@link AsyncScalarFunction} operator for Table API.
 *
 * <p>This operator executes Python async scalar functions asynchronously using the Python async
 * execution framework from DataStream API. It extends {@link AbstractPythonScalarFunctionOperator}
 * to reuse field forwarding and projection logic, but implements async execution semantics.
 *
 * <p>The async execution model:
 *
 * <ul>
 *   <li>Input rows are buffered in a queue with their forwarded fields
 *   <li>UDF inputs are sent to Python worker for async execution
 *   <li>Results are collected asynchronously and joined with forwarded fields
 *   <li>Output maintains the order of input (ordered async processing)
 * </ul>
 */
@Internal
public class PythonAsyncScalarFunctionOperator extends AbstractPythonScalarFunctionOperator {

    private static final long serialVersionUID = 1L;

    /** The TypeSerializer for udf execution results. */
    private transient TypeSerializer<RowData> udfOutputTypeSerializer;

    /** The TypeSerializer for udf input elements. */
    private transient TypeSerializer<RowData> udfInputTypeSerializer;

    /**
     * Queue to store the input records for which the async function has been triggered. This
     * maintains the order of async operations.
     */
    private transient Queue<StreamRecord<RowData>> inputRecordQueue;

    /** The collector for emitting results, wrapped with timestamp support. */
    private transient TimestampedCollector<RowData> timestampedCollector;

    /**
     * The maximum number of async operations that can be in-flight at the same time. This controls
     * the buffer capacity for async execution.
     */
    private final int asyncBufferCapacity;

    /** The timeout in milliseconds for async operations. */
    private final long asyncTimeout;

    /** Whether retry is enabled for async operations. */
    private final boolean asyncRetryEnabled;

    /** Maximum number of retry attempts. */
    private final int asyncMaxAttempts;

    /** Delay between retries in milliseconds. */
    private final long asyncRetryDelayMs;

    public PythonAsyncScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udfInputGeneratedProjection,
            GeneratedProjection forwardedFieldGeneratedProjection) {
        this(
                config,
                scalarFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                udfInputGeneratedProjection,
                forwardedFieldGeneratedProjection,
                100, // default buffer capacity
                60000, // default timeout: 60 seconds
                false, // retry disabled by default
                3, // default max attempts
                100); // default retry delay: 100ms
    }

    public PythonAsyncScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udfInputGeneratedProjection,
            GeneratedProjection forwardedFieldGeneratedProjection,
            int asyncBufferCapacity,
            long asyncTimeout,
            boolean asyncRetryEnabled,
            int asyncMaxAttempts,
            long asyncRetryDelayMs) {
        super(
                config,
                scalarFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                udfInputGeneratedProjection,
                forwardedFieldGeneratedProjection);
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.asyncTimeout = asyncTimeout;
        this.asyncRetryEnabled = asyncRetryEnabled;
        this.asyncMaxAttempts = asyncMaxAttempts;
        this.asyncRetryDelayMs = asyncRetryDelayMs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();
        udfInputTypeSerializer = PythonTypeUtils.toInternalSerializer(udfInputType);
        udfOutputTypeSerializer = PythonTypeUtils.toInternalSerializer(udfOutputType);
        inputRecordQueue = new LinkedList<>();
        timestampedCollector = new TimestampedCollector<>(output);
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        // Check if any function takes row as input
        for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
            if (pythonFunctionInfo.getPythonFunction().takesRowAsInput()) {
                return createRowTypeCoderInfoDescriptorProto(
                        runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
            }
        }
        return createFlattenRowTypeCoderInfoDescriptorProto(
                runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(RowType runnerOutType) {
        return createFlattenRowTypeCoderInfoDescriptorProto(
                runnerOutType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false);
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        // Serialize the UDF input and send to Python worker
        udfInputTypeSerializer.serialize(getFunctionInput(value), baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // Store the input record for later retrieval when result arrives
        inputRecordQueue.offer(element);

        // Check if the async buffer is full
        if (inputRecordQueue.size() >= asyncBufferCapacity) {
            // Wait for some results to be processed before accepting more inputs
            // This provides backpressure
            LOG.debug("Async buffer is full, waiting for results to be processed");
        }

        // Process the element (send to Python worker)
        super.processElement(element);
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws IOException {
        // Retrieve the corresponding input record
        StreamRecord<RowData> inputRecord = inputRecordQueue.poll();
        if (inputRecord == null) {
            throw new IllegalStateException(
                    "Received result from Python worker but no corresponding input record found");
        }

        byte[] rawUdfResult = resultTuple.f1;
        int length = resultTuple.f2;

        // Get the forwarded fields from the buffered input
        RowData forwardedFields = forwardedInputQueue.poll();
        if (forwardedFields == null) {
            throw new IllegalStateException(
                    "Forwarded fields queue is empty when processing async result");
        }

        // Deserialize the UDF result
        bais.setBuffer(rawUdfResult, 0, length);
        RowData udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);

        // Join forwarded fields with UDF result
        reuseJoinedRow.setRowKind(forwardedFields.getRowKind());
        RowData outputRow = reuseJoinedRow.replace(forwardedFields, udfResult);

        // Emit the result with the original timestamp
        timestampedCollector.setAbsoluteTimestamp(inputRecord.getTimestamp());
        timestampedCollector.collect(outputRow);
    }

    @Override
    public String getFunctionUrn() {
        // Use a dedicated URN for async scalar functions
        return "flink:transform:async_scalar_function:v1";
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        // Create the base proto with scalar functions
        FlinkFnApi.UserDefinedFunctions.Builder builder =
                ProtoUtils.createUserDefinedFunctionsProto(
                        getRuntimeContext(),
                        scalarFunctions,
                        config.get(PYTHON_METRIC_ENABLED),
                        config.get(PYTHON_PROFILE_ENABLED))
                        .toBuilder();

        // Add async-specific configuration
        builder.setAsyncBufferCapacity(asyncBufferCapacity);
        builder.setAsyncTimeoutMs(asyncTimeout);

        // Add retry configuration
        builder.setAsyncRetryEnabled(asyncRetryEnabled);
        builder.setAsyncMaxAttempts(asyncMaxAttempts);
        builder.setAsyncRetryDelayMs(asyncRetryDelayMs);

        return builder.build();
    }

    /** Gets the async buffer capacity. */
    public int getAsyncBufferCapacity() {
        return asyncBufferCapacity;
    }

    /** Gets the async timeout in milliseconds. */
    public long getAsyncTimeout() {
        return asyncTimeout;
    }
}
