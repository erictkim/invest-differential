package com.invest.differential.operator;

import com.invest.differential.zset.RowPredicate;
import com.invest.differential.zset.ZSet;

/**
 * Filter operator (WHERE clause). Linear — inherently incremental.
 * Applies predicate to each delta row; preserves weights.
 */
public final class FilterOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final RowPredicate predicate;

    public FilterOperator(Stream input, RowPredicate predicate) {
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.predicate = predicate;
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        output.setValue(delta.filter(predicate));
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Filter"; }
}
