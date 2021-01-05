package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class InternalTableDescriptor implements TableDescriptor {

    public List<String> partitionedFields;

    public Schema schema;

    public final Map<String, String> options;

    InternalTableDescriptor() {
        this.options = new HashMap<>();
    }

    @Override
    public List<String> getPartitionedFields() {
        return partitionedFields;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }
}
