package com.invest.differential.operator;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Incremental EXCEPT (set difference) operator.
 *
 * Maintains accumulated left and right inputs. On each step:
 * 1. Adds deltas to accumulated state
 * 2. Computes except = distinct(left) - distinct(right), then distinct
 * 3. Diffs with previous output to produce delta
 */
public final class ExceptOperator implements Operator {

    private final Stream left;
    private final Stream right;
    private final Stream output;
    private final BufferAllocator allocator;

    private ZSet accumulatedLeft;
    private ZSet accumulatedRight;
    private ZSet previousResult;

    public ExceptOperator(Stream left, Stream right, BufferAllocator allocator) {
        this.left = left;
        this.right = right;
        this.output = new Stream(left.dataSchema());
        this.allocator = allocator;
        this.accumulatedLeft = ZSet.empty(left.dataSchema(), allocator);
        this.accumulatedRight = ZSet.empty(right.dataSchema(), allocator);
        this.previousResult = ZSet.empty(left.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet deltaLeft = left.getValue();
        ZSet deltaRight = right.getValue();

        // Accumulate both sides
        ZSet newLeft = accumulatedLeft.add(deltaLeft);
        newLeft.compact();
        accumulatedLeft.close();
        accumulatedLeft = newLeft;

        ZSet newRight = accumulatedRight.add(deltaRight);
        newRight.compact();
        accumulatedRight.close();
        accumulatedRight = newRight;

        // Compute except: distinct(left) \ distinct(right)
        ZSet currentResult = accumulatedLeft.except(accumulatedRight);

        // Diff with previous output
        ZSet deltaOutput = currentResult.subtract(previousResult);
        deltaOutput.compact();
        previousResult.close();
        previousResult = currentResult;

        output.setValue(deltaOutput);
    }

    @Override
    public void reset() {
        if (accumulatedLeft != null) accumulatedLeft.close();
        if (accumulatedRight != null) accumulatedRight.close();
        if (previousResult != null) previousResult.close();
        accumulatedLeft = ZSet.empty(left.dataSchema(), allocator);
        accumulatedRight = ZSet.empty(right.dataSchema(), allocator);
        previousResult = ZSet.empty(left.dataSchema(), allocator);
        output.clear();
    }

    @Override
    public void close() {
        if (accumulatedLeft != null) { accumulatedLeft.close(); accumulatedLeft = null; }
        if (accumulatedRight != null) { accumulatedRight.close(); accumulatedRight = null; }
        if (previousResult != null) { previousResult.close(); previousResult = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Except"; }
}
