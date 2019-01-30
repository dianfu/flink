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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Metadata;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.SchematicDescriptor;
import org.apache.flink.table.descriptors.Statistics;
import org.apache.flink.table.descriptors.StreamableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_COLUMNS;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_ROW_COUNT;
import static org.apache.flink.table.descriptors.StatisticsValidator.readColumnStats;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;

/**
 * Defines a table in an {@link ExternalCatalog}. External catalog tables describe table sources
 * and/or sinks for both batch and stream environments.
 *
 * <p>See also {@link TableFactory} for more information about how to target suitable factories.
 *
 * <p>Use {@link ExternalCatalogTableBuilder} to integrate with the normalized descriptor-based API.
 */
@PublicEvolving
public class ExternalCatalogTable extends TableDescriptor {

	/**
	 * Flag whether this external table is intended for batch environments.
	 */
	private final boolean isBatch;

	/**
	 * Flag whether this external table is intended for streaming environments.
	 */
	private final boolean isStreaming;

	/**
	 * Flag whether this external table is declared as table source.
	 */
	private final boolean isSource;

	/**
	 * Flag whether this external table is declared as table sink.
	 */
	private final boolean isSink;

	/**
	 * Properties that describe the table and should match with a {@link TableFactory}.
	 */
	private final Map<String, String> properties;

	public ExternalCatalogTable(
		boolean isBatch,
		boolean isStreaming,
		boolean isSource,
		boolean isSink,
		Map<String, String> properties) {
		this.isBatch = isBatch;
		this.isStreaming = isStreaming;
		this.isSource = isSource;
		this.isSink = isSink;
		this.properties = properties;
	}

	// ----------------------------------------------------------------------------------------------
	// Legacy code
	// ---------------------------------------------------------------------------------------------

	/**
	 * Reads table statistics from the descriptors properties.
	 *
	 * @deprecated This method exists for backwards-compatibility only.
	 */
	@Deprecated
	public Optional<TableStats> getTableStats() {
		DescriptorProperties normalizedProps = new DescriptorProperties();
		normalizedProps.putProperties(normalizedProps);
		Optional<Long> rowCount = normalizedProps.getOptionalLong(STATISTICS_ROW_COUNT);
		if (rowCount.isPresent()) {
			Map<String, ColumnStats> columnStats = readColumnStats(normalizedProps, STATISTICS_COLUMNS);
			return Optional.of(new TableStats(rowCount.get(), columnStats));
		} else {
			return Optional.empty();
		}
	}

	// ----------------------------------------------------------------------------------------------
	// Getters
	// ----------------------------------------------------------------------------------------------

	/**
	 * Returns whether this external table is declared as table source.
	 */
	public boolean isTableSource() {
		return isSource;
	}

	/**
	 * Returns whether this external table is declared as table sink.
	 */
	public boolean isTableSink() {
		return isSink;
	}

	/**
	 * Returns whether this external table is intended for batch environments.
	 */
	public boolean isBatchTable() {
		return isBatch;
	}

	/**
	 * Returns whether this external table is intended for stream environments.
	 */
	public boolean isStreamTable() {
		return isStreaming;
	}

	// ----------------------------------------------------------------------------------------------

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		return properties;
	}

	/**
	 * Creates a builder for creating an {@link ExternalCatalogTable}.
	 *
	 * <p>It takes {@link Descriptor}s which allow for declaring the communication to external
	 * systems in an implementation-agnostic way. The classpath is scanned for suitable table
	 * factories that match the desired configuration.
	 *
	 * <p>Use the provided builder methods to configure the external catalog table accordingly.
	 *
	 * <p>The following example shows how to read from a connector using a JSON format and
	 * declaring it as a table source:
	 *
	 * <p>{@code
	 * new ExternalCatalogTable(
	 *   new ExternalSystemXYZ()
	 *     .version("0.11"))
	 * .withFormat(
	 *   new Json()
	 *     .jsonSchema("{...}")
	 *     .failOnMissingField(false))
	 * .withSchema(
	 *   new Schema()
	 *     .field("user-name", "VARCHAR").from("u_name")
	 *     .field("count", "DECIMAL"))
	 * .supportsStreaming()
	 * .asTableSource()
	 * }
	 *
	 * @param connectorDescriptor Connector descriptor describing the external system
	 * @return External catalog builder
	 */
	public static ExternalCatalogTableBuilder builder(ConnectorDescriptor connectorDescriptor) {
		return new ExternalCatalogTableBuilder(connectorDescriptor);
	}

	/**
	 * Builder for an {@link ExternalCatalogTable}.
	 */
	public static class ExternalCatalogTableBuilder
		extends TableDescriptor
		implements SchematicDescriptor<ExternalCatalogTableBuilder>, StreamableDescriptor<ExternalCatalogTableBuilder> {

		/**
		 * Connector descriptor describing the external system.
		 */
		private ConnectorDescriptor connectorDescriptor;
		private boolean isBatch = true;
		private boolean isStreaming = true;

		private FormatDescriptor formatDescriptor;
		private Schema schemaDescriptor;
		private Statistics statisticsDescriptor;
		private Metadata metadataDescriptor;
		private String updateMode;

		ExternalCatalogTableBuilder(ConnectorDescriptor connectorDescriptor) {
			this.connectorDescriptor = connectorDescriptor;
		}

		/**
		 * Specifies the format that defines how to read data from a connector.
		 */

		@Override
		public ExternalCatalogTableBuilder withFormat(FormatDescriptor format) {
			formatDescriptor = format;
			return this;
		}

		/**
		 * Specifies the resulting table schema.
		 */
		@Override
		public ExternalCatalogTableBuilder withSchema(Schema schema) {
			schemaDescriptor = schema;
			return this;
		}

		/**
		 * Declares how to perform the conversion between a dynamic table and an external connector.
		 *
		 * <p>In append mode, a dynamic table and an external connector only exchange INSERT messages.
		 *
		 * @see #inRetractMode()
		 * @see #inUpsertMode()
		 */
		@Override
		public ExternalCatalogTableBuilder inAppendMode() {
			updateMode = UPDATE_MODE_VALUE_APPEND;
			return this;
		}

		/**
		 * Declares how to perform the conversion between a dynamic table and an external connector.
		 *
		 * <p>In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages.
		 *
		 * <p>An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
		 * UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
		 * the updating (new) row.
		 *
		 * <p>In this mode, a key must not be defined as opposed to upsert mode. However, every update
		 * consists of two messages which is less efficient.
		 *
		 * @see #inAppendMode()
		 * @see #inUpsertMode()
		 */
		@Override
		public ExternalCatalogTableBuilder inRetractMode() {
			updateMode = UPDATE_MODE_VALUE_RETRACT;
			return this;
		}

		/**
		 * Declares how to perform the conversion between a dynamic table and an external connector.
		 *
		 * <p>In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages.
		 *
		 * <p>This mode requires a (possibly composite) unique key by which updates can be propagated. The
		 * external connector needs to be aware of the unique key attribute in order to apply messages
		 * correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
		 * DELETE messages.
		 *
		 * <p>The main difference to a retract stream is that UPDATE changes are encoded with a single
		 * message and are therefore more efficient.
		 *
		 * @see #inAppendMode()
		 * @see #inRetractMode()
		 */
		public ExternalCatalogTableBuilder inUpsertMode() {
			updateMode = UPDATE_MODE_VALUE_UPSERT;
			return this;
		}

		/**
		 * Specifies the statistics for this external table.
		 */
		public ExternalCatalogTableBuilder withStatistics(Statistics statistics) {
			statisticsDescriptor = statistics;
			return this;
		}

		/**
		 * Specifies the metadata for this external table.
		 */
		public ExternalCatalogTableBuilder withMetadata(Metadata metadata) {
			metadataDescriptor = metadata;
			return this;
		}

		/**
		 * Explicitly declares this external table for supporting only stream environments.
		 */
		public ExternalCatalogTableBuilder supportsStreaming() {
			isBatch = false;
			isStreaming = true;
			return this;
		}

		/**
		 * Explicitly declares this external table for supporting only batch environments.
		 */
		public ExternalCatalogTableBuilder supportsBatch() {
			isBatch = true;
			isStreaming = false;
			return this;
		}

		/**
		 * Explicitly declares this external table for supporting both batch and stream environments.
		 */
		public ExternalCatalogTableBuilder supportsBatchAndStreaming() {
			isBatch = true;
			isStreaming = true;
			return this;
		}

		/**
		 * Declares this external table as a table source and returns the
		 * configured {@link ExternalCatalogTable}.
		 *
		 * @return External catalog table
		 */
		public ExternalCatalogTable asTableSource() {
			return new ExternalCatalogTable(
				isBatch,
				isStreaming,
				true,
				false,
				toProperties());
		}

		/**
		 * Declares this external table as a table sink and returns the
		 * configured {@link ExternalCatalogTable}.
		 *
		 * @return External catalog table
		 */
		public ExternalCatalogTable asTableSink() {
			return new ExternalCatalogTable(
				isBatch,
				isStreaming,
				false,
				true,
				toProperties());
		}

		/**
		 * Declares this external table as both a table source and sink. It returns the
		 * configured {@link ExternalCatalogTable}.
		 *
		 * @return External catalog table
		 */
		public ExternalCatalogTable asTableSourceAndSink() {
			return new ExternalCatalogTable(
				isBatch,
				isStreaming,
				true,
				true,
				toProperties());
		}

		// ----------------------------------------------------------------------------------------------

		/**
		 * Converts this descriptor into a set of properties.
		 */
		@Override
		public Map<String, String> toProperties() {
			DescriptorProperties properties = new DescriptorProperties();
			properties.putProperties(connectorDescriptor.toProperties());
			if (formatDescriptor != null) {
				properties.putProperties(formatDescriptor.toProperties());
			}
			if (schemaDescriptor != null) {
				properties.putProperties(schemaDescriptor.toProperties());
			}
			if (statisticsDescriptor != null) {
				properties.putProperties(statisticsDescriptor.toProperties());
			}
			if (metadataDescriptor != null) {
				properties.putProperties(metadataDescriptor.toProperties());
			}
			if (updateMode != null) {
				properties.putString(UPDATE_MODE, updateMode);
			}
			return properties.asMap();
		}
	}
}
