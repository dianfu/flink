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

/**
 * The base interface of runner which is responsible for the execution of Python functions.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public interface PythonFunctionRunner<IN> {

	/**
	 * Prepares the Python function runner, such as preparing the Python execution environment, etc.
	 * After open method is called, the runner is ready to process input elements.
	 */
	void open() throws Exception;

	/**
	 * Tear-down the Python UDF runner.
	 */
	void close() throws Exception;

	void startBundle() throws Exception;

	void finishBundle() throws Exception;

	/**
	 * Executes the Python function with the input element.
	 */
	void processElement(IN element);
}
