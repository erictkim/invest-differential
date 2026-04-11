package com.invest.differential.zset;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.arrow.RowHasher;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/**
 * A Z-set: a multiset with integer weights, backed by Apache Arrow columnar storage.
 *
 * <p>Each entry is a tuple of data column values plus a weight. Positive weights
 * represent insertions, negative weights represent deletions. Zero-weight entries
 * are removed during compaction.
 *
 * <p>Z-sets form a commutative group under addition (add/negate/zero).
 */
public final class ZSet implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Schema dataSchema;      // schema of data columns only (no weight)
    private final Schema fullSchema;      // schema with weight column
    private VectorSchemaRoot data;        // data columns + weight column
    private boolean compact;

    private ZSet(BufferAllocator allocator, Schema dataSchema, VectorSchemaRoot data, boolean compact) {
        this.allocator = allocator;
        this.dataSchema = dataSchema;
        this.fullSchema = data.getSchema();
        this.data = data;
        this.compact = compact;
    }

    // ---- Factories ----

    public static ZSet empty(Schema dataSchema, BufferAllocator allocator) {
        Schema full = ArrowUtils.createSchemaWithWeight(dataSchema);
        VectorSchemaRoot root = ArrowUtils.createEmpty(full, allocator);
        return new ZSet(allocator, dataSchema, root, true);
    }

    /**
     * Create a ZSet from an existing VectorSchemaRoot that already has the weight column.
     * The root is NOT copied — the ZSet takes ownership.
     */
    public static ZSet fromRoot(Schema dataSchema, VectorSchemaRoot rootWithWeight, BufferAllocator allocator) {
        return new ZSet(allocator, dataSchema, rootWithWeight, false);
    }

    /**
     * Create a ZSet from data rows, each with weight 1.
     */
    public static ZSet fromData(VectorSchemaRoot dataRoot, BufferAllocator allocator) {
        Schema dataSchema = dataRoot.getSchema();
        Schema full = ArrowUtils.createSchemaWithWeight(dataSchema);
        VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
        root.allocateNew();
        int rowCount = dataRoot.getRowCount();
        int weightCol = root.getFieldVectors().size() - 1;
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < dataRoot.getFieldVectors().size(); col++) {
                ArrowUtils.setValue(root.getVector(col), row, ArrowUtils.getValue(dataRoot.getVector(col), row));
            }
            ((IntVector) root.getVector(weightCol)).setSafe(row, 1);
        }
        root.setRowCount(rowCount);
        return new ZSet(allocator, dataSchema, root, false);
    }

    /**
     * Create a ZSet from raw Object[][] data rows, each with weight 1.
     * Convenience factory for testing and programmatic use.
     */
    public static ZSet fromData(Schema dataSchema, BufferAllocator allocator, Object[][] rows) {
        Schema full = ArrowUtils.createSchemaWithWeight(dataSchema);
        VectorSchemaRoot root = VectorSchemaRoot.create(full, allocator);
        root.allocateNew();
        int weightIdx = root.getFieldVectors().size() - 1;
        for (int row = 0; row < rows.length; row++) {
            for (int col = 0; col < rows[row].length; col++) {
                ArrowUtils.setValue(root.getVector(col), row, rows[row][col]);
            }
            ((IntVector) root.getVector(weightIdx)).setSafe(row, 1);
        }
        root.setRowCount(rows.length);
        return new ZSet(allocator, dataSchema, root, false);
    }

    // ---- Accessors ----

    public Schema dataSchema() { return dataSchema; }
    public Schema fullSchema() { return fullSchema; }
    public BufferAllocator allocator() { return allocator; }
    public VectorSchemaRoot data() { return data; }
    public VectorSchemaRoot getRoot() { return data; }
    public int rawRowCount() { return data.getRowCount(); }
    public int rowCount() { return data.getRowCount(); }

    public int getWeight(int rowIndex) {
        IntVector wv = (IntVector) data.getVector(ArrowUtils.WEIGHT_COLUMN);
        return wv.get(rowIndex);
    }

    public Object[] getDataValues(int rowIndex) {
        int dataCols = dataSchema.getFields().size();
        Object[] values = new Object[dataCols];
        for (int i = 0; i < dataCols; i++) {
            values[i] = ArrowUtils.getValue(data.getVector(i), rowIndex);
        }
        return values;
    }

    // ---- Compaction ----

    /**
     * Merge duplicate rows (sum weights) and remove zero-weight rows.
     */
    public ZSet compact() {
        if (compact) return this;

        int[] dataCols = ArrowUtils.dataColumnIndices(data);
        int weightColIdx = data.getFieldVectors().size() - 1;
        int rowCount = data.getRowCount();

        // Build hash map: hash -> list of (row indices in output)
        Map<Integer, List<int[]>> hashMap = new HashMap<>(); // hash -> [(outRowIndex, weight)]
        VectorSchemaRoot out = VectorSchemaRoot.create(fullSchema, allocator);
        out.allocateNew();
        int outRow = 0;

        for (int row = 0; row < rowCount; row++) {
            int hash = RowHasher.hashRow(data, row, dataCols);
            int weight = ((IntVector) data.getVector(weightColIdx)).get(row);

            List<int[]> bucket = hashMap.get(hash);
            boolean merged = false;
            if (bucket != null) {
                for (int[] entry : bucket) {
                    if (RowHasher.rowsEqual(data, row, out, entry[0], dataCols)) {
                        entry[1] = Math.addExact(entry[1], weight);
                        merged = true;
                        break;
                    }
                }
            }
            if (!merged) {
                // Copy row to output
                ArrowUtils.copyRow(data, row, out, outRow);
                if (bucket == null) {
                    bucket = new ArrayList<>(2);
                    hashMap.put(hash, bucket);
                }
                bucket.add(new int[]{outRow, weight});
                outRow++;
            }
        }

        // Now write final weights and compact out zero-weight entries
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int finalRow = 0;
        for (List<int[]> bucket : hashMap.values()) {
            for (int[] entry : bucket) {
                if (entry[1] != 0) {
                    ArrowUtils.copyRow(out, entry[0], result, finalRow);
                    ((IntVector) result.getVector(weightColIdx)).setSafe(finalRow, entry[1]);
                    finalRow++;
                }
            }
        }
        result.setRowCount(finalRow);
        out.close();

        VectorSchemaRoot old = this.data;
        this.data = result;
        this.compact = true;
        old.close();
        return this;
    }

    public int entryCount() {
        compact();
        return data.getRowCount();
    }

    public boolean isEmpty() {
        compact();
        return data.getRowCount() == 0;
    }

    // ---- Algebraic Operations (Commutative Group) ----

    /**
     * Z-set addition: pointwise sum of weights.
     */
    public ZSet add(ZSet other) {
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();

        int thisRows = this.data.getRowCount();
        int otherRows = other.data.getRowCount();
        int total = thisRows + otherRows;

        // Concatenate both
        for (int row = 0; row < thisRows; row++) {
            ArrowUtils.copyRow(this.data, row, result, row);
        }
        for (int row = 0; row < otherRows; row++) {
            ArrowUtils.copyRow(other.data, row, result, thisRows + row);
        }
        result.setRowCount(total);

        return new ZSet(allocator, dataSchema, result, false);
    }

    /**
     * Negate all weights.
     */
    public ZSet negate() {
        VectorSchemaRoot result = ArrowUtils.cloneRoot(data, allocator);
        int weightColIdx = result.getFieldVectors().size() - 1;
        IntVector wv = (IntVector) result.getVector(weightColIdx);
        for (int row = 0; row < result.getRowCount(); row++) {
            wv.setSafe(row, Math.negateExact(wv.get(row)));
        }
        return new ZSet(allocator, dataSchema, result, compact);
    }

    /**
     * Subtract: this - other = this + (-other).
     */
    public ZSet subtract(ZSet other) {
        ZSet negated = other.negate();
        ZSet result = this.add(negated);
        negated.close();
        return result;
    }

    // ---- Relational Operations ----

    /**
     * Filter: keep rows matching predicate, preserve weights.
     */
    public ZSet filter(RowPredicate predicate) {
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int outRow = 0;
        for (int row = 0; row < data.getRowCount(); row++) {
            if (predicate.test(data, row)) {
                ArrowUtils.copyRow(data, row, result, outRow);
                outRow++;
            }
        }
        result.setRowCount(outRow);
        return new ZSet(allocator, dataSchema, result, compact);
    }

    /**
     * Map/projection: transform each row, preserve weights. May need compaction.
     */
    public ZSet map(Schema outputDataSchema, RowMapper mapper) {
        Schema outFull = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int weightColIdx = data.getFieldVectors().size() - 1;
        int outWeightColIdx = result.getFieldVectors().size() - 1;

        for (int row = 0; row < data.getRowCount(); row++) {
            Object[] outValues = mapper.map(data, row);
            for (int col = 0; col < outValues.length; col++) {
                ArrowUtils.setValue(result.getVector(col), row, outValues[col]);
            }
            int weight = ((IntVector) data.getVector(weightColIdx)).get(row);
            ((IntVector) result.getVector(outWeightColIdx)).setSafe(row, weight);
        }
        result.setRowCount(data.getRowCount());
        return new ZSet(allocator, outputDataSchema, result, false);
    }

    /**
     * Distinct: keep rows with positive weight, set weight to 1.
     */
    public ZSet distinct() {
        return positive(true);
    }

    /**
     * Keep rows with positive weight, preserving original weights.
     */
    public ZSet positive() {
        return positive(false);
    }

    /**
     * Keep rows with positive weight. If asSet is true, clamp weight to 1.
     */
    public ZSet positive(boolean asSet) {
        compact();
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int weightColIdx = data.getFieldVectors().size() - 1;
        int outRow = 0;
        for (int row = 0; row < data.getRowCount(); row++) {
            int weight = ((IntVector) data.getVector(weightColIdx)).get(row);
            if (weight > 0) {
                ArrowUtils.copyRow(data, row, result, outRow);
                if (asSet) {
                    ((IntVector) result.getVector(weightColIdx)).setSafe(outRow, 1);
                }
                outRow++;
            }
        }
        result.setRowCount(outRow);
        return new ZSet(allocator, dataSchema, result, true);
    }

    /**
     * Index/group-by: partition rows by key columns → IndexedZSet.
     */
    public IndexedZSet index(int[] keyColumnIndices) {
        return IndexedZSet.fromZSet(this, keyColumnIndices, allocator);
    }

    /**
     * Cartesian product / multiply: cross product with weight multiplication.
     */
    public ZSet multiply(ZSet other, Schema outputDataSchema, RowCombiner combiner) {
        Schema outFull = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int otherWeightCol = other.data.getFieldVectors().size() - 1;
        int outWeightCol = result.getFieldVectors().size() - 1;

        int outRow = 0;
        for (int r1 = 0; r1 < this.data.getRowCount(); r1++) {
            int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(r1);
            for (int r2 = 0; r2 < other.data.getRowCount(); r2++) {
                int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(r2);
                int wResult = Math.multiplyExact(w1, w2);
                if (wResult != 0) {
                    Object[] outValues = combiner.combine(this.data, r1, other.data, r2);
                    for (int col = 0; col < outValues.length; col++) {
                        ArrowUtils.setValue(result.getVector(col), outRow, outValues[col]);
                    }
                    ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, wResult);
                    outRow++;
                }
            }
        }
        result.setRowCount(outRow);
        return new ZSet(allocator, outputDataSchema, result, false);
    }

    /**
     * Scalar multiply: scale all weights by a factor.
     */
    public ZSet multiply(int factor) {
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int weightColIdx = data.getFieldVectors().size() - 1;
        for (int row = 0; row < data.getRowCount(); row++) {
            ArrowUtils.copyRow(data, row, result, row);
            int oldWeight = ((IntVector) data.getVector(weightColIdx)).get(row);
            ((IntVector) result.getVector(weightColIdx)).setSafe(row, Math.multiplyExact(oldWeight, factor));
        }
        result.setRowCount(data.getRowCount());
        return new ZSet(allocator, dataSchema, result, false);
    }

    // ---- Set Operations ----

    public ZSet union(ZSet other) {
        return this.add(other).distinct();
    }

    public ZSet unionAll(ZSet other) {
        return this.add(other);
    }

    public ZSet except(ZSet other) {
        return this.distinct().subtract(other.distinct()).distinct();
    }

    public ZSet intersect(ZSet other) {
        // intersect = distinct(A) + distinct(B), then keep weight >= 2, then distinct
        ZSet dA = this.distinct();
        ZSet dB = other.distinct();
        ZSet sum = dA.add(dB);
        sum.compact();
        int weightColIdx = sum.data.getFieldVectors().size() - 1;
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int outRow = 0;
        for (int row = 0; row < sum.data.getRowCount(); row++) {
            int weight = ((IntVector) sum.data.getVector(weightColIdx)).get(row);
            if (weight >= 2) {
                ArrowUtils.copyRow(sum.data, row, result, outRow);
                ((IntVector) result.getVector(weightColIdx)).setSafe(outRow, 1);
                outRow++;
            }
        }
        result.setRowCount(outRow);
        dA.close();
        dB.close();
        sum.close();
        return new ZSet(allocator, dataSchema, result, true);
    }

    // ---- Aggregation ----

    /**
     * Aggregate over all rows with weight-aware accumulation.
     */
    public <R, IR> R aggregate(AggregateDescription<R, IR> agg) {
        compact();
        IR acc = agg.initialValue();
        int dataCols = dataSchema.getFields().size();
        int weightColIdx = data.getFieldVectors().size() - 1;
        for (int row = 0; row < data.getRowCount(); row++) {
            Object[] values = new Object[dataCols];
            for (int col = 0; col < dataCols; col++) {
                values[col] = ArrowUtils.getValue(data.getVector(col), row);
            }
            int weight = ((IntVector) data.getVector(weightColIdx)).get(row);
            acc = agg.accumulator().accumulate(acc, values, weight);
        }
        return agg.finalizer().apply(acc);
    }

    // ---- Equality ----

    /**
     * Two Z-sets are equal iff their difference is empty (the "zero" element).
     */
    public boolean equalsZSet(ZSet other) {
        ZSet diff = this.subtract(other);
        boolean result = diff.isEmpty();
        diff.close();
        return result;
    }

    /**
     * Append the entries of another ZSet into this one (mutating).
     * Used by IntegrateOperator for efficient accumulation.
     */
    public void appendInPlace(ZSet other) {
        int thisRows = this.data.getRowCount();
        int otherRows = other.data.getRowCount();
        int total = thisRows + otherRows;

        for (int row = 0; row < otherRows; row++) {
            ArrowUtils.copyRow(other.data, row, this.data, thisRows + row);
        }
        this.data.setRowCount(total);
        this.compact = false;
    }

    @Override
    public void close() {
        if (data != null) {
            data.close();
            data = null;
        }
    }

    @Override
    public String toString() {
        if (data == null) return "ZSet(closed)";
        compact();
        StringBuilder sb = new StringBuilder("ZSet{");
        int weightColIdx = data.getFieldVectors().size() - 1;
        int dataCols = dataSchema.getFields().size();
        for (int row = 0; row < data.getRowCount(); row++) {
            if (row > 0) sb.append(", ");
            sb.append("[");
            for (int col = 0; col < dataCols; col++) {
                if (col > 0) sb.append(", ");
                sb.append(ArrowUtils.getValue(data.getVector(col), row));
            }
            sb.append("]=>").append(((IntVector) data.getVector(weightColIdx)).get(row));
        }
        sb.append("}");
        return sb.toString();
    }
}
