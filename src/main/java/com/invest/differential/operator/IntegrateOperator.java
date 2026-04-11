package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Integration operator: running sum I[t] = I[t-1] + Δ[t].
 * Stateful. Accumulates all input deltas into the full collection.
 *
 * Input: stream of deltas (ΔZ-sets)
 * Output: the accumulated full collection at each step
 */
public final class IntegrateOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;
    private ZSet state; // running accumulated Z-set

    public IntegrateOperator(Stream input, BufferAllocator allocator) {
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.allocator = allocator;
        this.state = ZSet.empty(input.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        ZSet newState = state.add(delta);
        newState.compact();
        state.close();
        state = newState;
        // Clone state for output so the stream owns its own copy
        output.setValue(ZSet.fromRoot(state.dataSchema(),
                ArrowUtils.cloneRoot(state.data(), allocator), allocator));
    }

    @Override
    public void reset() {
        if (state != null) state.close();
        state = ZSet.empty(input.dataSchema(), allocator);
        output.clear();
    }

    @Override
    public void close() {
        if (state != null) { state.close(); state = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Integrate"; }

    public ZSet currentState() { return state; }
}
