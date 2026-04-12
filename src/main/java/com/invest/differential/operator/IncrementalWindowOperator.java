package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/**
 * Incremental window operator (SQL OVER clause).
 *
 * <p>Maintains the full accumulated input. On each step:
 * <ol>
 *   <li>Adds ΔInput to accumulated state</li>
 *   <li>Recomputes full window function output over accumulated state</li>
 *   <li>Diffs with previous output to produce ΔOutput</li>
 * </ol>
 *
 * <p>Supports PARTITION BY, ORDER BY, and window functions:
 * ROW_NUMBER, RANK, DENSE_RANK, SUM, COUNT, MIN, MAX.
 * Supports window frames: ROWS BETWEEN ... AND ...
 */
public final class IncrementalWindowOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;
    private final Schema inputDataSchema;
    private final Schema outputDataSchema;
    private final int[] partitionColumns;
    private final int[] orderColumns;
    private final boolean[] orderAscending;
    private final List<WindowFunctionSpec> windowFunctions;

    private ZSet accumulatedInput;
    private ZSet previousOutput;

    // Cached per-partition output keyed by partition key. Enables incremental recompute.
    private Map<List<Object>, ZSet> partitionOutputCache;

    public IncrementalWindowOperator(Stream input,
                                     Schema outputDataSchema,
                                     int[] partitionColumns,
                                     int[] orderColumns,
                                     boolean[] orderAscending,
                                     List<WindowFunctionSpec> windowFunctions,
                                     BufferAllocator allocator) {
        this.input = input;
        this.inputDataSchema = input.dataSchema();
        this.outputDataSchema = outputDataSchema;
        this.output = new Stream(outputDataSchema);
        this.allocator = allocator;
        this.partitionColumns = partitionColumns;
        this.orderColumns = orderColumns;
        this.orderAscending = orderAscending;
        this.windowFunctions = windowFunctions;
        this.accumulatedInput = ZSet.empty(inputDataSchema, allocator);
        this.previousOutput = ZSet.empty(outputDataSchema, allocator);
        this.partitionOutputCache = new LinkedHashMap<>();
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();

        // Accumulate: state += Δ
        ZSet newState = accumulatedInput.add(delta);
        newState.compact();
        accumulatedInput.close();
        accumulatedInput = newState;

        if (accumulatedInput.isEmpty()) {
            // Invalidate all cached partitions
            for (ZSet cached : partitionOutputCache.values()) cached.close();
            partitionOutputCache.clear();
            ZSet emptyOut = ZSet.empty(outputDataSchema, allocator);
            ZSet deltaOutput = emptyOut.subtract(previousOutput);
            deltaOutput.compact();
            previousOutput.close();
            previousOutput = emptyOut;
            output.setValue(deltaOutput);
            return;
        }

        // Determine which partitions were touched by the delta
        Set<List<Object>> touchedPartitions = collectPartitionKeys(delta);

        // Recompute only touched partitions; reuse cached output for untouched ones
        Map<List<Object>, List<Integer>> allPartitions = buildPartitions(accumulatedInput);

        // For touched partitions, recompute; also handle partitions that disappeared
        for (List<Object> key : touchedPartitions) {
            ZSet old = partitionOutputCache.remove(key);
            if (old != null) old.close();
        }

        // Recompute touched partitions from accumulated state
        for (List<Object> key : touchedPartitions) {
            List<Integer> rows = allPartitions.get(key);
            if (rows != null && !rows.isEmpty()) {
                ZSet partOut = computePartitionOutput(accumulatedInput.data(), rows);
                partitionOutputCache.put(key, partOut);
            }
            // If rows == null, partition disappeared — already removed from cache above
        }

        // Build full output from cache
        ZSet fullOutput = mergePartitionOutputs();

        // Diff: ΔOutput = fullOutput - previousOutput
        ZSet deltaOutput = fullOutput.subtract(previousOutput);
        deltaOutput.compact();
        previousOutput.close();
        previousOutput = fullOutput;

        output.setValue(deltaOutput);
    }

    /**
     * Collect the set of partition keys present in a delta ZSet.
     */
    private Set<List<Object>> collectPartitionKeys(ZSet delta) {
        Set<List<Object>> keys = new LinkedHashSet<>();
        VectorSchemaRoot root = delta.data();
        for (int r = 0; r < root.getRowCount(); r++) {
            keys.add(getPartitionKey(root, r));
        }
        return keys;
    }

    /**
     * Build partition groupings from the accumulated input.
     * Expands rows by weight and groups by partition key.
     */
    private Map<List<Object>, List<Integer>> buildPartitions(ZSet inputZSet) {
        VectorSchemaRoot inputRoot = inputZSet.data();
        int rowCount = inputRoot.getRowCount();
        int weightColIdx = inputRoot.getFieldVectors().size() - 1;

        List<Integer> expandedRows = new ArrayList<>();
        for (int r = 0; r < rowCount; r++) {
            int weight = ((IntVector) inputRoot.getVector(weightColIdx)).get(r);
            for (int w = 0; w < weight; w++) {
                expandedRows.add(r);
            }
        }

        Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
        for (int idx : expandedRows) {
            List<Object> key = getPartitionKey(inputRoot, idx);
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
        }

        // Sort each partition by order columns
        for (List<Integer> partition : partitions.values()) {
            partition.sort((a, b) -> compareRows(inputRoot, a, b));
        }

        return partitions;
    }

    /**
     * Compute window output for a single partition (list of sorted expanded row indices).
     */
    private ZSet computePartitionOutput(VectorSchemaRoot inputRoot, List<Integer> partition) {
        int inputDataCols = inputDataSchema.getFields().size();
        Schema outFull = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot outRoot = VectorSchemaRoot.create(outFull, allocator);
        outRoot.allocateNew();
        int outWeightCol = outRoot.getFieldVectors().size() - 1;

        for (int posInPartition = 0; posInPartition < partition.size(); posInPartition++) {
            int srcRow = partition.get(posInPartition);

            for (int col = 0; col < inputDataCols; col++) {
                Object val = ArrowUtils.getValue(inputRoot.getVector(col), srcRow);
                ArrowUtils.setValue(outRoot.getVector(col), posInPartition, val);
            }

            for (int f = 0; f < windowFunctions.size(); f++) {
                WindowFunctionSpec spec = windowFunctions.get(f);
                Object result = computeWindowFunction(spec, inputRoot, partition, posInPartition);
                ArrowUtils.setValue(outRoot.getVector(inputDataCols + f), posInPartition, result);
            }

            ((IntVector) outRoot.getVector(outWeightCol)).setSafe(posInPartition, 1);
        }
        outRoot.setRowCount(partition.size());

        return ZSet.fromRoot(outputDataSchema, outRoot, allocator);
    }

    /**
     * Merge all partition outputs from the cache into a single ZSet.
     */
    private ZSet mergePartitionOutputs() {
        ZSet merged = ZSet.empty(outputDataSchema, allocator);
        for (ZSet partOut : partitionOutputCache.values()) {
            ZSet combined = merged.add(partOut);
            merged.close();
            merged = combined;
        }
        return merged;
    }

    @SuppressWarnings("unchecked")
    private Object computeWindowFunction(WindowFunctionSpec spec, VectorSchemaRoot root,
                                          List<Integer> partition, int currentPos) {
        return switch (spec.functionName()) {
            case "row_number" -> (long) (currentPos + 1);
            case "rank" -> {
                long rank = 1;
                for (int i = 0; i < currentPos; i++) {
                    if (compareRows(root, partition.get(i), partition.get(currentPos)) != 0) {
                        rank = i + 1;
                    }
                }
                if (currentPos > 0 && compareRows(root, partition.get(currentPos - 1), partition.get(currentPos)) != 0) {
                    rank = currentPos + 1;
                }
                yield rank;
            }
            case "dense_rank" -> {
                long denseRank = 1;
                for (int i = 1; i <= currentPos; i++) {
                    if (compareRows(root, partition.get(i - 1), partition.get(i)) != 0) {
                        denseRank++;
                    }
                }
                yield denseRank;
            }
            case "sum" -> {
                int col = spec.inputColumn();
                int[] bounds = resolveFrameBounds(spec, partition.size(), currentPos);
                long sum = 0;
                for (int i = bounds[0]; i <= bounds[1]; i++) {
                    Object val = ArrowUtils.getValue(root.getVector(col), partition.get(i));
                    if (val instanceof Number n) {
                        sum += n.longValue();
                    }
                }
                yield sum;
            }
            case "count" -> {
                int[] bounds = resolveFrameBounds(spec, partition.size(), currentPos);
                long count = 0;
                if (spec.inputColumn() >= 0) {
                    int col = spec.inputColumn();
                    for (int i = bounds[0]; i <= bounds[1]; i++) {
                        Object val = ArrowUtils.getValue(root.getVector(col), partition.get(i));
                        if (val != null) count++;
                    }
                } else {
                    count = bounds[1] - bounds[0] + 1;
                }
                yield count;
            }
            case "min" -> {
                int col = spec.inputColumn();
                int[] bounds = resolveFrameBounds(spec, partition.size(), currentPos);
                Comparable<Object> min = null;
                for (int i = bounds[0]; i <= bounds[1]; i++) {
                    Object val = ArrowUtils.getValue(root.getVector(col), partition.get(i));
                    if (val != null) {
                        Comparable<Object> cval = (Comparable<Object>) val;
                        if (min == null || cval.compareTo((Object) min) < 0) {
                            min = cval;
                        }
                    }
                }
                yield (Object) min;
            }
            case "max" -> {
                int col = spec.inputColumn();
                int[] bounds = resolveFrameBounds(spec, partition.size(), currentPos);
                Comparable<Object> max = null;
                for (int i = bounds[0]; i <= bounds[1]; i++) {
                    Object val = ArrowUtils.getValue(root.getVector(col), partition.get(i));
                    if (val != null) {
                        Comparable<Object> cval = (Comparable<Object>) val;
                        if (max == null || cval.compareTo((Object) max) > 0) {
                            max = cval;
                        }
                    }
                }
                yield (Object) max;
            }
            default -> throw new UnsupportedOperationException("Unsupported window function: " + spec.functionName());
        };
    }

    /**
     * Resolve frame bounds for a given partition size and current position.
     * Returns [startIdx, endIdx] (inclusive) within the partition.
     */
    private int[] resolveFrameBounds(WindowFunctionSpec spec, int partitionSize, int currentPos) {
        int start;
        int end;

        switch (spec.lowerBoundType()) {
            case UNBOUNDED_PRECEDING -> start = 0;
            case CURRENT_ROW -> start = currentPos;
            case PRECEDING -> start = Math.max(0, currentPos - spec.lowerBoundOffset());
            case FOLLOWING -> start = Math.min(partitionSize - 1, currentPos + spec.lowerBoundOffset());
            default -> start = 0;
        }

        switch (spec.upperBoundType()) {
            case UNBOUNDED_FOLLOWING -> end = partitionSize - 1;
            case CURRENT_ROW -> end = currentPos;
            case PRECEDING -> end = Math.max(0, currentPos - spec.upperBoundOffset());
            case FOLLOWING -> end = Math.min(partitionSize - 1, currentPos + spec.upperBoundOffset());
            default -> end = partitionSize - 1;
        }

        return new int[]{start, end};
    }

    private List<Object> getPartitionKey(VectorSchemaRoot root, int row) {
        if (partitionColumns.length == 0) {
            return List.of(); // single partition
        }
        Object[] key = new Object[partitionColumns.length];
        for (int i = 0; i < partitionColumns.length; i++) {
            key[i] = ArrowUtils.getValue(root.getVector(partitionColumns[i]), row);
        }
        return List.of(key);
    }

    @SuppressWarnings("unchecked")
    private int compareRows(VectorSchemaRoot root, int rowA, int rowB) {
        for (int i = 0; i < orderColumns.length; i++) {
            int col = orderColumns[i];
            Object a = ArrowUtils.getValue(root.getVector(col), rowA);
            Object b = ArrowUtils.getValue(root.getVector(col), rowB);
            int cmp;
            if (a == null && b == null) {
                cmp = 0;
            } else if (a == null) {
                cmp = 1; // nulls last
            } else if (b == null) {
                cmp = -1;
            } else {
                cmp = ((Comparable<Object>) a).compareTo(b);
            }
            if (!orderAscending[i]) {
                cmp = -cmp;
            }
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override
    public void reset() {
        if (accumulatedInput != null) accumulatedInput.close();
        if (previousOutput != null) previousOutput.close();
        clearPartitionCache();
        accumulatedInput = ZSet.empty(inputDataSchema, allocator);
        previousOutput = ZSet.empty(outputDataSchema, allocator);
        output.clear();
    }

    @Override
    public void close() {
        if (accumulatedInput != null) { accumulatedInput.close(); accumulatedInput = null; }
        if (previousOutput != null) { previousOutput.close(); previousOutput = null; }
        clearPartitionCache();
        output.clear();
    }

    private void clearPartitionCache() {
        if (partitionOutputCache != null) {
            for (ZSet cached : partitionOutputCache.values()) cached.close();
            partitionOutputCache.clear();
        }
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalWindow"; }

    // ---- Window function specification ----

    public enum BoundType {
        UNBOUNDED_PRECEDING,
        UNBOUNDED_FOLLOWING,
        CURRENT_ROW,
        PRECEDING,
        FOLLOWING
    }

    public record WindowFunctionSpec(
            String functionName,
            int inputColumn,       // -1 for functions without column arg (e.g. ROW_NUMBER, COUNT(*))
            BoundType lowerBoundType,
            int lowerBoundOffset,  // only used for PRECEDING/FOLLOWING
            BoundType upperBoundType,
            int upperBoundOffset   // only used for PRECEDING/FOLLOWING
    ) {
        public static WindowFunctionSpec ranking(String functionName) {
            return new WindowFunctionSpec(functionName, -1,
                    BoundType.UNBOUNDED_PRECEDING, 0,
                    BoundType.CURRENT_ROW, 0);
        }

        public static WindowFunctionSpec aggregate(String functionName, int inputColumn,
                                                    BoundType lowerType, int lowerOffset,
                                                    BoundType upperType, int upperOffset) {
            return new WindowFunctionSpec(functionName, inputColumn,
                    lowerType, lowerOffset, upperType, upperOffset);
        }
    }
}
