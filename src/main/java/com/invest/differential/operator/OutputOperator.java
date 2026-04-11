package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * External data sink operator. Read output via getValue().
 */
public final class OutputOperator implements Operator {

    private final String viewName;
    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;

    public OutputOperator(String viewName, Stream input, BufferAllocator allocator) {
        this.viewName = viewName;
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.allocator = allocator;
    }

    @Override
    public void step() {
        ZSet val = input.getValue();
        if (val != null) {
            output.setValue(ZSet.fromRoot(val.dataSchema(),
                    ArrowUtils.cloneRoot(val.data(), allocator), allocator));
        } else {
            output.setValue(null);
        }
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
