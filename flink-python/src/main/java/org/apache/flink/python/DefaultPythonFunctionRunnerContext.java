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

import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/**
 * A simple {@link PythonFunctionRunnerContext} implementation which
 * create a Python execution environment for each operator.
 */
@Internal
public class DefaultPythonFunctionRunnerContext implements PythonFunctionRunnerContext {

	public static DefaultPythonFunctionRunnerContext create(JobInfo jobInfo) {
		JobBundleFactory jobBundleFactory = DefaultJobBundleFactory.create(jobInfo);
		return new DefaultPythonFunctionRunnerContext(jobBundleFactory);
	}

	private final JobBundleFactory jobBundleFactory;

	private DefaultPythonFunctionRunnerContext(JobBundleFactory jobBundleFactory) {
		this.jobBundleFactory = jobBundleFactory;
	}

	@Override
	public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
		return jobBundleFactory.forStage(executableStage);
	}

	@Override
	public void close() throws Exception {
		jobBundleFactory.close();
	}
}
