package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Differentiation operator: D[t] = current - previous.
 * Stateful. Computes the delta (change) between consecutive values.
 *
 * Input: stream of full collections
 * Output: the delta (what changed since last step)
 */
public final class DifferentiateOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;
    private ZSet previous; // previous input value

    public DifferentiateOperator(Stream input, BufferAllocator allocator) {
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.allocator = allocator;
        this.previous = ZSet.empty(input.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet current = input.getValue();
        ZSet delta = current.subtract(previous);
        delta.compact();
        previous.close();
        // Clone current as our new previous state
        previous = ZSet.fromRoot(current.dataSchema(),
                ArrowUtils.cloneRoot(current.data(), allocator), allocator);
        output.setValue(delta);
    }

    @Override
    public void reset() {
        if (previous != null) previous.close();
        previous = ZSet.empty(input.dataSchema(), allocator);
        output.clear();
    }

    @Override
    public void close() {
        if (previous != null) { previous.close(); previous = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Differentiate"; }
}
