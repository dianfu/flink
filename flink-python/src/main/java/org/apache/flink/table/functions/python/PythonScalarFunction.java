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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionLanguage;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.python.PythonFunction;

import java.io.Serializable;
import java.util.Map;

/**
 * Python ScalarFunction.
 */
@Internal
public class PythonScalarFunction
	extends ScalarFunction
	implements PythonFunction, Serializable {

	private static final long serialVersionUID = 1L;

	private final String funcName;
	private final byte[] serializedScalarFunction;
	private final Map<String, String> pythonEnv;
	// TODO: respect inputTypes
	private final TypeInformation<?>[] inputTypes;
	private final TypeInformation<?> resultType;
	private final boolean deterministic;

	public PythonScalarFunction(
		String funcName,
		byte[] serializedScalarFunction,
		Map<String, String> pythonEnv,
		TypeInformation<?>[] inputTypes,
		TypeInformation<?> resultType,
		boolean deterministic) {
		this.funcName = funcName;
		this.serializedScalarFunction = serializedScalarFunction;
		this.pythonEnv = pythonEnv;
		this.inputTypes = inputTypes;
		this.resultType = resultType;
		this.deterministic = deterministic;
	}

	@Override
	public void open(FunctionContext context) {}

	@Override
	public void close() {}

	public Object eval(Object... args) throws RuntimeException {
		return null;
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return resultType;
	}

	@Override
	public FunctionLanguage getLanguage() {
		return FunctionLanguage.PYTHON;
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return serializedScalarFunction;
	}

	@Override
	public Map<String, String> getPythonEnv() {
		return pythonEnv;
	}

	@Override
	public boolean isDeterministic() {
		return deterministic;
	}

	public String toString() {
		return funcName;
	}
}
