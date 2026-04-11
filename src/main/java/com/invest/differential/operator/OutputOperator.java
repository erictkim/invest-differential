package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * External data sink operator. Read output via getValue().
 *
 * <p>Maintains both the latest delta and the accumulated snapshot (materialized view).
 * The snapshot is the sum of all deltas across all steps, compacted to remove
 * zero-weight entries.
 */
public final class OutputOperator implements Operator {

    private final String viewName;
    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;
    private ZSet snapshot; // accumulated output (full materialized view)

    public OutputOperator(String viewName, Stream input, BufferAllocator allocator) {
        this.viewName = viewName;
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.allocator = allocator;
        this.snapshot = ZSet.empty(input.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet val = input.getValue();
        if (val != null) {
            ZSet cloned = ZSet.fromRoot(val.dataSchema(),
                    ArrowUtils.cloneRoot(val.data(), allocator), allocator);
            output.setValue(cloned);

            // Accumulate into snapshot
            ZSet newSnapshot = snapshot.add(cloned);
            newSnapshot.compact();
            snapshot.close();
            snapshot = newSnapshot;
        } else {
            output.setValue(null);
        }
    }

    @Override
    public void reset() {
        output.clear();
        snapshot.close();
        snapshot = ZSet.empty(input.dataSchema(), allocator);
    }

    @Override
    public void close() {
        output.clear();
        if (snapshot != null) {
            snapshot.close();
            snapshot = null;
        }
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Output(" + viewName + ")"; }

    public ZSet getValue() { return output.getValue(); }

    /**
     * Get the full materialized snapshot — the accumulated result of all deltas.
     * The returned ZSet is compacted (no zero-weight entries).
     */
    public ZSet getSnapshot() { return snapshot; }
}
