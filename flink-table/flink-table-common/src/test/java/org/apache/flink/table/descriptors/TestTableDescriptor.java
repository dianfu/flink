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

package org.apache.flink.table.descriptors;

import java.util.Map;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;

/**
 * Tests for {@link TableDescriptor}.
 */
public class TestTableDescriptor extends TableDescriptor
	implements SchematicDescriptor<TestTableDescriptor>, StreamableDescriptor<TestTableDescriptor> {

	public final ConnectorDescriptor connector;
	private FormatDescriptor formatDescriptor;
	private Schema schemaDescriptor;
	private String updateMode;

	public TestTableDescriptor(ConnectorDescriptor connector) {
		this.connector = connector;
	}

	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();

		properties.putProperties(connector.toProperties());
		if (formatDescriptor != null) {
			properties.putProperties(formatDescriptor.toProperties());
		}
		if (schemaDescriptor != null) {
			properties.putProperties(schemaDescriptor.toProperties());
		}
		if (updateMode != null) {
			properties.putString(UPDATE_MODE, updateMode);
		}

		return properties.asMap();
	}

	@Override
	public TestTableDescriptor withFormat(FormatDescriptor format) {
		this.formatDescriptor = format;
		return this;
	}

	@Override
	public TestTableDescriptor withSchema(Schema schema) {
		this.schemaDescriptor = schema;
		return this;
	}

	@Override
	public TestTableDescriptor inAppendMode() {
		updateMode = UPDATE_MODE_VALUE_APPEND;
		return this;
	}

	@Override
	public TestTableDescriptor inRetractMode() {
		updateMode = UPDATE_MODE_VALUE_RETRACT;
		return this;
	}

	@Override
	public TestTableDescriptor inUpsertMode() {
		updateMode = UPDATE_MODE_VALUE_UPSERT;
		return this;
	}
}
