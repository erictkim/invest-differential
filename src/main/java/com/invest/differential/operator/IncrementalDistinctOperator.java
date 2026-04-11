package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Incremental distinct operator.
 *
 * Maintains the full accumulated input. On each step:
 * 1. Adds ΔInput to accumulated state
 * 2. Computes distinct(state)
 * 3. Diffs with previous distinct output to produce ΔOutput
 */
public final class IncrementalDistinctOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;

    private ZSet accumulatedInput;
    private ZSet previousDistinct;

    public IncrementalDistinctOperator(Stream input, BufferAllocator allocator) {
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.allocator = allocator;
        this.accumulatedInput = ZSet.empty(input.dataSchema(), allocator);
        this.previousDistinct = ZSet.empty(input.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();

        // Accumulate
        ZSet newState = accumulatedInput.add(delta);
        newState.compact();
        accumulatedInput.close();
        accumulatedInput = newState;

        // Compute distinct
        ZSet currentDistinct = accumulatedInput.distinct();

        // Diff: ΔOutput = currentDistinct - previousDistinct
        ZSet deltaOutput = currentDistinct.subtract(previousDistinct);
        deltaOutput.compact();
        previousDistinct.close();
        previousDistinct = currentDistinct;

        output.setValue(deltaOutput);
    }

    @Override
    public void reset() {
        if (accumulatedInput != null) accumulatedInput.close();
        if (previousDistinct != null) previousDistinct.close();
        accumulatedInput = ZSet.empty(input.dataSchema(), allocator);
        previousDistinct = ZSet.empty(input.dataSchema(), allocator);
        output.clear();
    }

    @Override
    public void close() {
        if (accumulatedInput != null) { accumulatedInput.close(); accumulatedInput = null; }
        if (previousDistinct != null) { previousDistinct.close(); previousDistinct = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalDistinct"; }
}
