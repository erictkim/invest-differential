package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.IndexedZSet;
import com.invest.differential.zset.RowCombiner;
import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Incremental equi-join operator.
 *
 * <p>For INNER joins, maintains integrated state for both inputs: I(Left), I(Right).
 * On each step with deltas ΔLeft, ΔRight:
 *   ΔResult = (ΔLeft ⋈ I_prev(Right)) + (I_new(Left) ⋈ ΔRight)
 * where I_new(Left) = I_prev(Left) + ΔLeft.
 * This avoids the double-counting term ΔLeft ⋈ ΔRight because I_new(Left) includes ΔLeft.
 *
 * <p>For LEFT, RIGHT, and FULL outer joins, uses an integrate-diff approach:
 * maintains full state and previous output, recomputes the full join on the
 * accumulated state, and diffs against the previous output to produce ΔResult.
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
    private final RowMapper unmatchedLeftMapper;   // produces output for unmatched left rows (LEFT/FULL)
    private final RowMapper unmatchedRightMapper;  // produces output for unmatched right rows (RIGHT/FULL)

    private ZSet leftState;   // I(Left): accumulated left deltas
    private ZSet rightState;  // I(Right): accumulated right deltas
    private ZSet previousOutput; // previous full join output (used for outer joins)

    public IncrementalJoinOperator(Stream leftInput, Stream rightInput,
                                    int[] leftKeyColumns, int[] rightKeyColumns,
                                    Schema outputDataSchema, RowCombiner valueCombiner,
                                    JoinType joinType, BufferAllocator allocator) {
        this(leftInput, rightInput, leftKeyColumns, rightKeyColumns,
                outputDataSchema, valueCombiner, joinType, allocator, null, null);
    }

    public IncrementalJoinOperator(Stream leftInput, Stream rightInput,
                                    int[] leftKeyColumns, int[] rightKeyColumns,
                                    Schema outputDataSchema, RowCombiner valueCombiner,
                                    JoinType joinType, BufferAllocator allocator,
                                    RowMapper unmatchedLeftMapper, RowMapper unmatchedRightMapper) {
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

        // Build default mappers if not provided
        int leftCols = leftInput.dataSchema().getFields().size();
        int totalCols = outputDataSchema.getFields().size();
        this.unmatchedLeftMapper = unmatchedLeftMapper != null ? unmatchedLeftMapper : (data, row) -> {
            Object[] vals = new Object[totalCols];
            for (int i = 0; i < leftCols && i < totalCols; i++) {
                vals[i] = ArrowUtils.getValue(data.getVector(i), row);
            }
            return vals;
        };
        this.unmatchedRightMapper = unmatchedRightMapper != null ? unmatchedRightMapper : (data, row) -> {
            int rightCols = data.getFieldVectors().size() - 1; // exclude weight
            Object[] vals = new Object[totalCols];
            int offset = totalCols - rightCols;
            for (int i = 0; i < rightCols && (offset + i) < totalCols; i++) {
                vals[offset + i] = ArrowUtils.getValue(data.getVector(i), row);
            }
            return vals;
        };

        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL
                || joinType == JoinType.LEFT) {
            this.previousOutput = ZSet.empty(outputDataSchema, allocator);
        } else {
            this.previousOutput = null;
        }
    }

    @Override
    public void step() {
        ZSet deltaLeft = leftInput.getValue();
        ZSet deltaRight = rightInput.getValue();

        if (joinType == JoinType.INNER) {
            stepInner(deltaLeft, deltaRight);
        } else {
            stepOuter(deltaLeft, deltaRight);
        }
    }

    /**
     * Inner join: bilinear incremental formula.
     * ΔResult = (ΔLeft ⋈ I_prev(Right)) + (I_new(Left) ⋈ ΔRight)
     */
    private void stepInner(ZSet deltaLeft, ZSet deltaRight) {
        // Part 1: ΔLeft ⋈ I_prev(Right)
        ZSet part1 = innerJoinZSets(deltaLeft, rightState);

        // Update left state: I_new(Left) = I_prev(Left) + ΔLeft
        ZSet newLeftState = leftState.add(deltaLeft);
        newLeftState.compact();
        leftState.close();
        leftState = newLeftState;

        // Part 2: I_new(Left) ⋈ ΔRight
        ZSet part2 = innerJoinZSets(leftState, deltaRight);

        // Update right state
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

    /**
     * Outer joins (LEFT, RIGHT, FULL): integrate-diff approach.
     * Accumulate state, recompute full join, diff with previous output.
     */
    private void stepOuter(ZSet deltaLeft, ZSet deltaRight) {
        // Update states
        ZSet newLeftState = leftState.add(deltaLeft);
        newLeftState.compact();
        leftState.close();
        leftState = newLeftState;

        ZSet newRightState = rightState.add(deltaRight);
        newRightState.compact();
        rightState.close();
        rightState = newRightState;

        // Compute full join on current state
        ZSet fullOutput = outerJoinZSets(leftState, rightState);
        fullOutput.compact();

        // Diff: ΔOutput = fullOutput - previousOutput
        ZSet deltaOutput = fullOutput.subtract(previousOutput);
        deltaOutput.compact();
        previousOutput.close();
        previousOutput = fullOutput;

        output.setValue(deltaOutput);
    }

    private ZSet innerJoinZSets(ZSet left, ZSet right) {
        if (left.isEmpty() || right.isEmpty()) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        IndexedZSet leftIdx = left.index(leftKeyColumns);
        IndexedZSet rightIdx = right.index(rightKeyColumns);

        IndexedZSet joined = leftIdx.join(rightIdx, outputDataSchema, valueCombiner);

        ZSet result = joined.deindex();
        leftIdx.close();
        rightIdx.close();
        joined.close();
        return result;
    }

    private ZSet outerJoinZSets(ZSet left, ZSet right) {
        if (left.isEmpty() && right.isEmpty()) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        // Handle empty sides for outer joins
        if (left.isEmpty() && joinType == JoinType.LEFT) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        if (right.isEmpty() && joinType == JoinType.RIGHT) {
            return ZSet.empty(outputDataSchema, allocator);
        }

        IndexedZSet leftIdx = left.index(leftKeyColumns);
        IndexedZSet rightIdx = right.index(rightKeyColumns);

        IndexedZSet joined;
        switch (joinType) {
            case LEFT:
                joined = leftIdx.leftJoin(rightIdx, outputDataSchema, valueCombiner, unmatchedLeftMapper);
                break;
            case RIGHT:
                joined = leftIdx.rightJoin(rightIdx, outputDataSchema, valueCombiner, unmatchedRightMapper);
                break;
            case FULL:
                joined = leftIdx.fullJoin(rightIdx, outputDataSchema, valueCombiner,
                        unmatchedLeftMapper, unmatchedRightMapper);
                break;
            default:
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
        if (previousOutput != null) previousOutput.close();
        leftState = ZSet.empty(leftInput.dataSchema(), allocator);
        rightState = ZSet.empty(rightInput.dataSchema(), allocator);
        if (joinType != JoinType.INNER) {
            previousOutput = ZSet.empty(outputDataSchema, allocator);
        }
        output.clear();
    }

    @Override
    public void close() {
        if (leftState != null) { leftState.close(); leftState = null; }
        if (rightState != null) { rightState.close(); rightState = null; }
        if (previousOutput != null) { previousOutput.close(); previousOutput = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalJoin(" + joinType + ")"; }
}
