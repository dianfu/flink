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

import java.util.Arrays;

/**
 * A basic builder implementation to build a {@link TableDescriptor}.
 */
@PublicEvolving
public abstract class TableDescriptorBuilder<BUILDER extends TableDescriptorBuilder<BUILDER>> {

    private final InternalTableDescriptor descriptor = new InternalTableDescriptor();

    /**
     * Returns the this builder instance in the type of subclass.
     */
    protected abstract BUILDER self();

    /**
     * Specifies the table schema.
     */
    public BUILDER schema(Schema schema) {
        descriptor.schema = schema;
        return self();
    }

    /**
     * Specifies the partition keys of this table.
     */
    public BUILDER partitionedBy(String... fieldNames) {
        descriptor.partitionedFields = Arrays.asList(fieldNames);
        return self();
    }

    protected BUILDER option(String key, String value) {
        descriptor.options.put(key, value);
        return self();
    }

    /**
     * Returns created table descriptor.
     */
    public TableDescriptor build() {
        return descriptor;
    }
}
