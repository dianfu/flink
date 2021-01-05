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

import java.util.List;
import java.util.Map;

/**
 * Describes a table to connect. It is a same representation of SQL CREATE TABLE DDL.
 * It wraps the needed meta information about a catalog table.
 * Please use a specific {@link TableDescriptorBuilder} to build the {@link TableDescriptor}.
 */
@PublicEvolving
public interface TableDescriptor {

    /**
     * Returns the partition keys of this table.
     */
    List<String> getPartitionedFields();

    /**
     * Returns the table schema.
     */
    Schema getSchema();

    /**
     * Returns the table options.
     */
    Map<String, String> getOptions();
}
