package com.invest.differential.operator;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A typed data edge between operators in the dataflow graph.
 * Holds the current ZSet value for a given step.
 */
public final class Stream {

    private final Schema dataSchema;
    private ZSet currentValue;

    public Stream(Schema dataSchema) {
        this.dataSchema = dataSchema;
    }

    public Schema dataSchema() { return dataSchema; }

    public ZSet getValue() { return currentValue; }

    public void setValue(ZSet value) {
        this.currentValue = value;
    }

    public void clear() {
        this.currentValue = null;
    }
}
