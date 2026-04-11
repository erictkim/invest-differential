package com.invest.differential.operator;

import com.invest.differential.zset.ZSet;

/**
 * Union all operator. Linear — inherently incremental.
 * Adds left and right deltas (bag union).
 */
public final class UnionAllOperator implements Operator {

    private final Stream left;
    private final Stream right;
    private final Stream output;

    public UnionAllOperator(Stream left, Stream right) {
        this.left = left;
        this.right = right;
        this.output = new Stream(left.dataSchema());
    }

    @Override
    public void step() {
        ZSet l = left.getValue();
        ZSet r = right.getValue();
        output.setValue(l.add(r));
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "UnionAll"; }
}
