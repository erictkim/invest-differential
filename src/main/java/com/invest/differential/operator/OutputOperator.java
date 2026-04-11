package com.invest.differential.operator;

import com.invest.differential.zset.ZSet;

/**
 * External data sink operator. Read output via getValue().
 */
public final class OutputOperator implements Operator {

    private final String viewName;
    private final Stream input;
    private final Stream output;

    public OutputOperator(String viewName, Stream input) {
        this.viewName = viewName;
        this.input = input;
        this.output = new Stream(input.dataSchema());
    }

    @Override
    public void step() {
        output.setValue(input.getValue());
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Output(" + viewName + ")"; }

    public ZSet getValue() { return output.getValue(); }
}
