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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for all stream operators to execute Python functions.
 */
@Internal
public abstract class AbstractPythonFunctionOperator<IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_MAX_BUNDLE_SIZE = 10000;
	private static final long DEFAULT_MAX_BUNDLE_TIME_MILLS = 10 * 1000; // 10s

	private transient PythonFunctionRunner<IN> pythonFunctionRunner;

	/**
	 * Use an AtomicBoolean because we start/stop bundles by a timer thread (see below).
	 */
	private transient AtomicBoolean bundleStarted;

	/**
	 * Number of processed elements in the current bundle.
	 */
	private transient int elementCount;

	/**
	 * Max number of elements to include in a bundle.
	 */
	private final int maxBundleSize;

	/**
	 * Max duration of a bundle.
	 */
	private final long maxBundleTimeMills;

	/**
	 * Time that the last bundle was finished.
	 */
	private transient long lastFinishBundleTime;

	/**
	 * A timer that finishes the current bundle after a fixed amount of time.
	 */
	private transient ScheduledFuture<?> checkFinishBundleTimer;

	public AbstractPythonFunctionOperator() {
		// TODO: makes the max bundle size and max bundle time configurable.
		this.maxBundleSize = DEFAULT_MAX_BUNDLE_SIZE;
		this.maxBundleTimeMills = DEFAULT_MAX_BUNDLE_TIME_MILLS;
	}

	@Override
	public void open() throws Exception {
		this.bundleStarted = new AtomicBoolean(false);

		this.pythonFunctionRunner = createPythonFunctionRunner();
		this.pythonFunctionRunner.open();

		this.elementCount = 0;
		this.lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();

		// Schedule timer to check timeout of finish bundle.
		long bundleCheckPeriod = Math.max(this.maxBundleTimeMills, 1);
		this.checkFinishBundleTimer =
			getProcessingTimeService()
				.scheduleAtFixedRate(
					timestamp -> checkInvokeFinishBundleByTime(), bundleCheckPeriod, bundleCheckPeriod);
	}

	@Override
	public void close() throws Exception {
		try {
			invokeFinishBundle();
		} finally {
			super.close();
		}
	}

	@Override
	public void dispose() throws Exception {
		try {
			if (checkFinishBundleTimer != null) {
				checkFinishBundleTimer.cancel(true);
				checkFinishBundleTimer = null;
			}
			if (pythonFunctionRunner != null) {
				pythonFunctionRunner.close();
				pythonFunctionRunner = null;
			}
		} finally {
			super.dispose();
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		checkInvokeStartBundle();
		pythonFunctionRunner.processElement(element.getValue());
		checkInvokeFinishBundleByCount();
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		// Ensure that no new bundle gets started
		while (bundleStarted.get()) {
			invokeFinishBundle();
		}

		super.prepareSnapshotPreBarrier(checkpointId);
	}

	public abstract PythonFunctionRunner<IN> createPythonFunctionRunner();

	public abstract void emitResults();

	/**
	 * Checks whether to invoke startBundle, if it is, need to output elements that were buffered as part
	 * of finishing a bundle in snapshot() first.
	 */
	private void checkInvokeStartBundle() throws Exception {
		if (bundleStarted.compareAndSet(false, true)) {
			pythonFunctionRunner.startBundle();
		}
	}

	/**
	 * Checks whether to invoke finishBundle by elements count. Called in processElement.
	 */
	private void checkInvokeFinishBundleByCount() throws Exception {
		elementCount++;
		if (elementCount >= maxBundleSize) {
			invokeFinishBundle();
		}
	}

	/**
	 * Check whether to invoke finishBundle by timeout.
	 */
	private void checkInvokeFinishBundleByTime() throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		if (now - lastFinishBundleTime >= maxBundleTimeMills) {
			invokeFinishBundle();
		}
	}

	private void invokeFinishBundle() throws Exception {
		if (bundleStarted.compareAndSet(true, false)) {
			pythonFunctionRunner.finishBundle();
			emitResults();
			elementCount = 0;
			lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
		}
	}
}
