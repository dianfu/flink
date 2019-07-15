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

package org.apache.flink.table.functions.python.coders;

import org.apache.flink.annotation.Internal;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} for BIGINT.
 */
@Internal
public class BigIntCoder extends Coder<Long> {

	private static final long serialVersionUID = 1L;

	public static final BigIntCoder INSTANCE = new BigIntCoder();

	@Override
	public void encode(Long value, OutputStream outStream) throws CoderException, IOException {
		VarInt.encode(value, outStream);
	}

	@Override
	public Long decode(InputStream inStream) throws CoderException, IOException {
		return VarInt.decodeLong(inStream);
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return Collections.emptyList();
	}

	@Override
	public void verifyDeterministic() throws NonDeterministicException {}
}
