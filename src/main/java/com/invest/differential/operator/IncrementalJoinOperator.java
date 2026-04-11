package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.IndexedZSet;
import com.invest.differential.zset.RowCombiner;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Incremental equi-join operator.
 *
 * Maintains integrated state for both inputs: I(Left), I(Right).
 * On each step with deltas ΔLeft, ΔRight:
 *   ΔResult = (ΔLeft ⋈ I_prev(Right)) + (I_new(Left) ⋈ ΔRight)
 * where I_new(Left) = I_prev(Left) + ΔLeft.
 *
 * This avoids the double-counting term ΔLeft ⋈ ΔRight because I_new(Left) includes ΔLeft.
 */
public final class IncrementalJoinOperator implements Operator {

    public enum JoinType { INNER, LEFT, RIGHT, FULL }

    private final Stream leftInput;
    private final Stream rightInput;
    private final Stream output;
    private final BufferAllocator allocator;
    private final int[] leftKeyColumns;
    private final int[] rightKeyColumns;
    private final Schema outputDataSchema;
    private final RowCombiner valueCombiner;
    private final JoinType joinType;

    private ZSet leftState;   // I(Left): accumulated left deltas
    private ZSet rightState;  // I(Right): accumulated right deltas

    public IncrementalJoinOperator(Stream leftInput, Stream rightInput,
                                    int[] leftKeyColumns, int[] rightKeyColumns,
                                    Schema outputDataSchema, RowCombiner valueCombiner,
                                    JoinType joinType, BufferAllocator allocator) {
        this.leftInput = leftInput;
        this.rightInput = rightInput;
        this.output = new Stream(outputDataSchema);
        this.allocator = allocator;
        this.leftKeyColumns = leftKeyColumns;
        this.rightKeyColumns = rightKeyColumns;
        this.outputDataSchema = outputDataSchema;
        this.valueCombiner = valueCombiner;
        this.joinType = joinType;
        this.leftState = ZSet.empty(leftInput.dataSchema(), allocator);
        this.rightState = ZSet.empty(rightInput.dataSchema(), allocator);
    }

    @Override
    public void step() {
        ZSet deltaLeft = leftInput.getValue();
        ZSet deltaRight = rightInput.getValue();

        // Part 1: ΔLeft ⋈ I_prev(Right)
        ZSet part1 = joinZSets(deltaLeft, rightState);

        // Update left state: I_new(Left) = I_prev(Left) + ΔLeft
        ZSet newLeftState = leftState.add(deltaLeft);
        newLeftState.compact();
        leftState.close();
        leftState = newLeftState;

        // Part 2: I_new(Left) ⋈ ΔRight
        ZSet part2 = joinZSets(leftState, deltaRight);

        // Update right state: I_new(Right) = I_prev(Right) + ΔRight
        ZSet newRightState = rightState.add(deltaRight);
        newRightState.compact();
        rightState.close();
        rightState = newRightState;

        // ΔResult = part1 + part2
        ZSet result = part1.add(part2);
        result.compact();
        part1.close();
        part2.close();

        output.setValue(result);
    }

    private ZSet joinZSets(ZSet left, ZSet right) {
        if (left.isEmpty() || right.isEmpty()) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        IndexedZSet leftIdx = left.index(leftKeyColumns);
        IndexedZSet rightIdx = right.index(rightKeyColumns);

        // Build the value combiner that maps from indexed (key+value) rows
        Schema leftValSchema = leftIdx.valueSchema();
        Schema rightValSchema = rightIdx.valueSchema();

        IndexedZSet joined;
        switch (joinType) {
            case INNER:
                joined = leftIdx.join(rightIdx, outputDataSchema, valueCombiner);
                break;
            case LEFT:
                Object[] nullRight = new Object[outputDataSchema.getFields().size()];
                joined = leftIdx.leftJoin(rightIdx, outputDataSchema, valueCombiner, nullRight);
                break;
            default:
                // For RIGHT join, swap sides and adjust combiner
                // For FULL, implement as LEFT ∪ anti-join(RIGHT)
                joined = leftIdx.join(rightIdx, outputDataSchema, valueCombiner);
                break;
        }

        ZSet result = joined.deindex();
        leftIdx.close();
        rightIdx.close();
        joined.close();
        return result;
    }

    @Override
    public void reset() {
        if (leftState != null) leftState.close();
        if (rightState != null) rightState.close();
        leftState = ZSet.empty(leftInput.dataSchema(), allocator);
        rightState = ZSet.empty(rightInput.dataSchema(), allocator);
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalJoin(" + joinType + ")"; }
}
