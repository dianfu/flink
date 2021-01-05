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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.WatermarkInfo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Describes a schema of a table.
 *
 * <p>Note: Field names are matched by the exact name by default (case sensitive).
 */
@PublicEvolving
public class Schema {

    // package visible variables used to build TableSchema
    protected final List<TableColumn> columns;
    protected final WatermarkInfo watermarkInfo;
    protected final List<String> primaryKey;

    private Schema(
            List<TableColumn> columns,
            WatermarkInfo watermarkInfo,
            List<String> primaryKey) {
        this.columns = columns;
        this.watermarkInfo = watermarkInfo;
        this.primaryKey = primaryKey;
    }

    public static SchemaBuilder newBuilder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {
        List<TableColumn> columns = new ArrayList<>();
        WatermarkInfo watermarkInfo;
        List<String> primaryKey;

        private SchemaBuilder() {
        }

        /**
         * Adds a column with the column name and the data type.
         */
        public SchemaBuilder column(String fieldName, AbstractDataType<?> fieldType) {
            columns.add(TableColumn.physical(fieldName, fieldType));
            return this;
        }

        public SchemaBuilder column(String fieldName, Expression expr) {
            columns.add(TableColumn.computed(fieldName, expr));
            return this;
        }

        public SchemaBuilder primaryKey(String... fieldNames) {
            this.primaryKey = Arrays.asList(fieldNames);
            return this;
        }

        public SchemaBuilder watermark(String rowtimeField, Expression watermarkExpr) {
            this.watermarkInfo = new WatermarkInfo(rowtimeField, watermarkExpr);
            return this;
        }

        public Schema build() {
            return new Schema(columns, watermarkInfo, primaryKey);
        }
    }
}
