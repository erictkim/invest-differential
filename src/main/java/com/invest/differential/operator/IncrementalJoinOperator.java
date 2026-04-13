package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.arrow.RowHasher;
import com.invest.differential.parallel.ParallelConfig;
import com.invest.differential.zset.IndexedZSet;
import com.invest.differential.zset.RowCombiner;
import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

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

    public enum JoinType { INNER, LEFT, RIGHT, FULL, SEMI, ANTI }

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
    private ParallelConfig parallelConfig = ParallelConfig.disabled();

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

        if (joinType != JoinType.INNER) {
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
            stepIntegrateDiff(deltaLeft, deltaRight);
        }
    }

    @Override
    public void setParallelConfig(ParallelConfig config) {
        this.parallelConfig = config != null ? config : ParallelConfig.disabled();
    }

    /**
     * Inner join: bilinear incremental formula.
     * ΔResult = (ΔLeft ⋈ I_prev(Right)) + (I_new(Left) ⋈ ΔRight)
     * When parallel config is enabled and data is large enough, hash-partitions
     * by join key and runs partitions on separate threads.
     */
    private void stepInner(ZSet deltaLeft, ZSet deltaRight) {
        int totalRows = deltaLeft.rowCount() + deltaRight.rowCount()
                + leftState.rowCount() + rightState.rowCount();

        if (parallelConfig.isEnabled()
                && totalRows >= parallelConfig.getMinRowsForDataParallel()
                && leftKeyColumns.length > 0) {
            stepInnerParallel(deltaLeft, deltaRight);
        } else {
            stepInnerSequential(deltaLeft, deltaRight);
        }
    }

    private void stepInnerSequential(ZSet deltaLeft, ZSet deltaRight) {
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

    private void stepInnerParallel(ZSet deltaLeft, ZSet deltaRight) {
        int n = Math.min(parallelConfig.getMaxParallelism(),
                Math.max(2, (deltaLeft.rowCount() + deltaRight.rowCount())
                        / parallelConfig.getMinRowsForDataParallel()));

        // Hash-partition all four inputs by join key
        ZSet[] dlParts = deltaLeft.hashPartition(leftKeyColumns, n);
        ZSet[] drParts = deltaRight.hashPartition(rightKeyColumns, n);
        ZSet[] lsParts = leftState.hashPartition(leftKeyColumns, n);
        ZSet[] rsParts = rightState.hashPartition(rightKeyColumns, n);

        // Run N independent join instances in parallel
        List<ForkJoinTask<ZSet[]>> tasks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final int part = i;
            tasks.add(parallelConfig.getPool().submit(new RecursiveTask<ZSet[]>() {
                @Override
                protected ZSet[] compute() {
                    // Part 1: ΔLeft[p] ⋈ I_Right[p]
                    ZSet p1 = innerJoinZSets(dlParts[part], rsParts[part]);

                    // Update left state partition
                    ZSet newLS = lsParts[part].add(dlParts[part]);
                    newLS.compact();
                    lsParts[part].close();

                    // Part 2: I_Left_new[p] ⋈ ΔRight[p]
                    ZSet p2 = innerJoinZSets(newLS, drParts[part]);

                    // Update right state partition
                    ZSet newRS = rsParts[part].add(drParts[part]);
                    newRS.compact();
                    rsParts[part].close();

                    // Result for this partition
                    ZSet partResult = p1.add(p2);
                    partResult.compact();
                    p1.close();
                    p2.close();
                    dlParts[part].close();
                    drParts[part].close();

                    return new ZSet[]{partResult, newLS, newRS};
                }
            }));
        }

        // Collect results and merge state
        ZSet[] resultParts = new ZSet[n];
        ZSet[] newLeftParts = new ZSet[n];
        ZSet[] newRightParts = new ZSet[n];
        for (int i = 0; i < n; i++) {
            ZSet[] partResults = tasks.get(i).join();
            resultParts[i] = partResults[0];
            newLeftParts[i] = partResults[1];
            newRightParts[i] = partResults[2];
        }

        // Merge partitioned results
        ZSet result = ZSet.concat(resultParts, outputDataSchema, allocator);
        result.compact();
        for (ZSet r : resultParts) r.close();

        // Merge partitioned state back
        leftState.close();
        leftState = ZSet.concat(newLeftParts, leftInput.dataSchema(), allocator);
        for (ZSet s : newLeftParts) s.close();

        rightState.close();
        rightState = ZSet.concat(newRightParts, rightInput.dataSchema(), allocator);
        for (ZSet s : newRightParts) s.close();

        output.setValue(result);
    }

    /**
     * Non-inner joins (LEFT, RIGHT, FULL, SEMI, ANTI): integrate-diff approach.
     * Accumulate state, recompute full join, diff with previous output.
     */
    private void stepIntegrateDiff(ZSet deltaLeft, ZSet deltaRight) {
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
        ZSet fullOutput = computeFullJoin(leftState, rightState);
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

    private ZSet computeFullJoin(ZSet left, ZSet right) {
        if (left.isEmpty() && right.isEmpty()) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        if (left.isEmpty() && (joinType == JoinType.LEFT || joinType == JoinType.SEMI || joinType == JoinType.ANTI)) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        if (right.isEmpty() && joinType == JoinType.RIGHT) {
            return ZSet.empty(outputDataSchema, allocator);
        }
        // Anti-join with empty right: all left rows pass
        if (right.isEmpty() && joinType == JoinType.ANTI) {
            return left.map(outputDataSchema, unmatchedLeftMapper);
        }
        // Semi-join with empty right: no rows pass
        if (right.isEmpty() && joinType == JoinType.SEMI) {
            return ZSet.empty(outputDataSchema, allocator);
        }

        // SEMI/ANTI use direct key matching without IndexedZSet
        if (joinType == JoinType.SEMI) {
            return filterByKeyMatch(left, right, true);
        }
        if (joinType == JoinType.ANTI) {
            return filterByKeyMatch(left, right, false);
        }

        IndexedZSet leftIdx = left.index(leftKeyColumns);
        IndexedZSet rightIdx = right.index(rightKeyColumns);

        ZSet result;
        switch (joinType) {
            case LEFT: {
                IndexedZSet joined = leftIdx.leftJoin(rightIdx, outputDataSchema, valueCombiner, unmatchedLeftMapper);
                result = joined.deindex();
                joined.close();
                break;
            }
            case RIGHT: {
                IndexedZSet joined = leftIdx.rightJoin(rightIdx, outputDataSchema, valueCombiner, unmatchedRightMapper);
                result = joined.deindex();
                joined.close();
                break;
            }
            case FULL: {
                IndexedZSet joined = leftIdx.fullJoin(rightIdx, outputDataSchema, valueCombiner,
                        unmatchedLeftMapper, unmatchedRightMapper);
                result = joined.deindex();
                joined.close();
                break;
            }
            default: {
                IndexedZSet joined = leftIdx.join(rightIdx, outputDataSchema, valueCombiner);
                result = joined.deindex();
                joined.close();
                break;
            }
        }

        leftIdx.close();
        rightIdx.close();
        return result;
    }

    /**
     * Filter left rows by whether their keys match any right row.
     * @param keepMatched true for semi-join (keep matched), false for anti-join (keep unmatched)
     */
    private ZSet filterByKeyMatch(ZSet left, ZSet right, boolean keepMatched) {
        VectorSchemaRoot rightData = right.data();
        Map<Integer, List<Integer>> rightByHash = new HashMap<>();
        for (int row = 0; row < rightData.getRowCount(); row++) {
            int h = RowHasher.hashRow(rightData, row, rightKeyColumns);
            rightByHash.computeIfAbsent(h, k -> new ArrayList<>()).add(row);
        }

        VectorSchemaRoot leftData = left.data();
        Schema outFullSchema = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFullSchema, allocator);
        result.allocateNew();

        int leftWeightCol = leftData.getFieldVectors().size() - 1;
        int outWeightCol = result.getFieldVectors().size() - 1;
        int outRow = 0;

        for (int leftRow = 0; leftRow < leftData.getRowCount(); leftRow++) {
            int leftHash = RowHasher.hashRow(leftData, leftRow, leftKeyColumns);
            boolean matched = false;

            List<Integer> candidates = rightByHash.get(leftHash);
            if (candidates != null) {
                for (int rightRow : candidates) {
                    boolean keysEqual = true;
                    for (int k = 0; k < leftKeyColumns.length; k++) {
                        Object lv = ArrowUtils.getValue(leftData.getVector(leftKeyColumns[k]), leftRow);
                        Object rv = ArrowUtils.getValue(rightData.getVector(rightKeyColumns[k]), rightRow);
                        if (!Objects.equals(lv, rv)) { keysEqual = false; break; }
                    }
                    if (keysEqual) { matched = true; break; }
                }
            }

            if (matched == keepMatched) {
                Object[] vals = unmatchedLeftMapper.map(leftData, leftRow);
                for (int i = 0; i < vals.length; i++) {
                    ArrowUtils.setValue(result.getVector(i), outRow, vals[i]);
                }
                int weight = ((IntVector) leftData.getVector(leftWeightCol)).get(leftRow);
                ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, weight);
                outRow++;
            }
        }
        result.setRowCount(outRow);
        return ZSet.fromRoot(outputDataSchema, result, allocator);
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
