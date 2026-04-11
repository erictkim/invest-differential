package com.invest.differential.operator;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * External data source operator. Feed changes via setValue().
 */
public final class InputOperator implements Operator {

    private final String tableName;
    private final Stream output;
    private final BufferAllocator allocator;

    public InputOperator(String tableName, Schema dataSchema, BufferAllocator allocator) {
        this.tableName = tableName;
        this.output = new Stream(dataSchema);
        this.allocator = allocator;
    }

    public void setValue(ZSet delta) {
        output.setValue(delta);
    }

    @Override
    public void step() {
        // InputOperator's value is set externally; if no value set, emit empty
        if (output.getValue() == null) {
            output.setValue(ZSet.empty(output.dataSchema(), allocator));
        }
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Input(" + tableName + ")"; }

    public String tableName() { return tableName; }
}
