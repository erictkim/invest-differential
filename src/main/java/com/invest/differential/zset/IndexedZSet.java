package com.invest.differential.zset;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.arrow.RowHasher;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/**
 * An indexed Z-set: Map&lt;Key, ZSet&lt;Value&gt;&gt; stored in columnar Arrow format.
 *
 * <p>Internally stored as a single VectorSchemaRoot with key columns + value columns + weight column.
 * Logically represents rows grouped by key, where each group is a Z-set of value tuples.
 */
public final class IndexedZSet implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final int[] keyColumnIndices;
    private final int[] valueColumnIndices;
    private final Schema fullSchema;   // key + value + weight
    private VectorSchemaRoot data;
    private int[] rowKeyHashes;         // cached per-row key hash

    private IndexedZSet(BufferAllocator allocator, Schema keySchema, Schema valueSchema,
                        int[] keyColumnIndices, int[] valueColumnIndices,
                        Schema fullSchema, VectorSchemaRoot data, int[] rowKeyHashes) {
        this.allocator = allocator;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyColumnIndices = keyColumnIndices;
        this.valueColumnIndices = valueColumnIndices;
        this.fullSchema = fullSchema;
        this.data = data;
        this.rowKeyHashes = rowKeyHashes;
    }

    // ---- Factories ----

    /**
     * Build an IndexedZSet from a ZSet by partitioning on key columns.
     * The data is stored as-is (key cols + remaining value cols + weight).
     */
    public static IndexedZSet fromZSet(ZSet zset, int[] keyColumnIndices, BufferAllocator allocator) {
        Schema dataSchema = zset.dataSchema();
        int dataCols = dataSchema.getFields().size();

        // Determine value column indices (all data columns not in key)
        Set<Integer> keySet = new HashSet<>();
        for (int k : keyColumnIndices) keySet.add(k);
        List<Integer> valCols = new ArrayList<>();
        for (int i = 0; i < dataCols; i++) {
            if (!keySet.contains(i)) valCols.add(i);
        }
        int[] valueColumnIndices = valCols.stream().mapToInt(Integer::intValue).toArray();

        Schema keySchema = ArrowUtils.subSchema(dataSchema, keyColumnIndices);
        Schema valueSchema = ArrowUtils.subSchema(dataSchema, valueColumnIndices);

        // Build new root: key columns, then value columns, then weight
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        for (int k : keyColumnIndices) fields.add(dataSchema.getFields().get(k));
        for (int v : valueColumnIndices) fields.add(dataSchema.getFields().get(v));
        fields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema fullSchema = new Schema(fields);

        int[] newKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < keyColumnIndices.length; i++) newKeyIndices[i] = i;
        int[] newValueIndices = new int[valueColumnIndices.length];
        for (int i = 0; i < valueColumnIndices.length; i++) newValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int weightSrcCol = zset.data().getFieldVectors().size() - 1;
        int weightDstCol = fields.size() - 1;

        int rowCount = zset.data().getRowCount();
        for (int row = 0; row < rowCount; row++) {
            // Copy key columns
            for (int i = 0; i < keyColumnIndices.length; i++) {
                ArrowUtils.setValue(result.getVector(i), row,
                        ArrowUtils.getValue(zset.data().getVector(keyColumnIndices[i]), row));
            }
            // Copy value columns
            for (int i = 0; i < valueColumnIndices.length; i++) {
                ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), row,
                        ArrowUtils.getValue(zset.data().getVector(valueColumnIndices[i]), row));
            }
            // Copy weight
            int weight = ((IntVector) zset.data().getVector(weightSrcCol)).get(row);
            ((IntVector) result.getVector(weightDstCol)).setSafe(row, weight);
        }
        result.setRowCount(rowCount);

        int[] hashes = computeKeyHashes(result, newKeyIndices);
        return new IndexedZSet(allocator, keySchema, valueSchema, newKeyIndices, newValueIndices,
                fullSchema, result, hashes);
    }

    public static IndexedZSet empty(Schema keySchema, Schema valueSchema, BufferAllocator allocator) {
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        fields.addAll(keySchema.getFields());
        fields.addAll(valueSchema.getFields());
        fields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema fullSchema = new Schema(fields);
        int[] keyIndices = new int[keySchema.getFields().size()];
        for (int i = 0; i < keyIndices.length; i++) keyIndices[i] = i;
        int[] valueIndices = new int[valueSchema.getFields().size()];
        for (int i = 0; i < valueIndices.length; i++) valueIndices[i] = keyIndices.length + i;
        VectorSchemaRoot root = ArrowUtils.createEmpty(fullSchema, allocator);
        return new IndexedZSet(allocator, keySchema, valueSchema, keyIndices, valueIndices, fullSchema, root, new int[0]);
    }

    // ---- Accessors ----

    public Schema keySchema() { return keySchema; }
    public Schema valueSchema() { return valueSchema; }
    public Schema fullSchema() { return fullSchema; }
    public int[] keyColumnIndices() { return keyColumnIndices; }
    public int[] valueColumnIndices() { return valueColumnIndices; }
    public VectorSchemaRoot data() { return data; }
    public BufferAllocator allocator() { return allocator; }

    public int getWeight(int rowIndex) {
        IntVector wv = (IntVector) data.getVector(ArrowUtils.WEIGHT_COLUMN);
        return wv.get(rowIndex);
    }

    // ---- Group operations ----

    /**
     * Add two IndexedZSets (merge rows, will need compaction via toZSet).
     */
    public IndexedZSet add(IndexedZSet other) {
        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int thisRows = this.data.getRowCount();
        int otherRows = other.data.getRowCount();
        for (int row = 0; row < thisRows; row++) {
            ArrowUtils.copyRow(this.data, row, result, row);
        }
        for (int row = 0; row < otherRows; row++) {
            ArrowUtils.copyRow(other.data, row, result, thisRows + row);
        }
        result.setRowCount(thisRows + otherRows);
        int[] mergedHashes = new int[thisRows + otherRows];
        System.arraycopy(this.rowKeyHashes, 0, mergedHashes, 0, thisRows);
        System.arraycopy(other.rowKeyHashes, 0, mergedHashes, thisRows, otherRows);
        return new IndexedZSet(allocator, keySchema, valueSchema, keyColumnIndices, valueColumnIndices,
                fullSchema, result, mergedHashes);
    }

    public IndexedZSet negate() {
        VectorSchemaRoot result = ArrowUtils.cloneRoot(data, allocator);
        int weightColIdx = result.getFieldVectors().size() - 1;
        IntVector wv = (IntVector) result.getVector(weightColIdx);
        for (int row = 0; row < result.getRowCount(); row++) {
            wv.setSafe(row, Math.negateExact(wv.get(row)));
        }
        return new IndexedZSet(allocator, keySchema, valueSchema, keyColumnIndices, valueColumnIndices,
                fullSchema, result, rowKeyHashes.clone());
    }

    // ---- Join ----

    /**
     * Equi-join: for matching keys, cross-product the value Z-sets with weight multiplication.
     */
    public IndexedZSet join(IndexedZSet other, Schema outputValueSchema,
                            RowCombiner valueCombiner) {
        // Build key groups for both sides
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        // Output schema: key + outputValue + weight
        List<org.apache.arrow.vector.types.pojo.Field> outFields = new ArrayList<>();
        outFields.addAll(keySchema.getFields());
        outFields.addAll(outputValueSchema.getFields());
        outFields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema outFull = new Schema(outFields);
        int[] outKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < outKeyIndices.length; i++) outKeyIndices[i] = i;
        int[] outValueIndices = new int[outputValueSchema.getFields().size()];
        for (int i = 0; i < outValueIndices.length; i++) outValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int outWeightCol = outFields.size() - 1;
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int otherWeightCol = other.data.getFieldVectors().size() - 1;
        int outRow = 0;

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());
            if (rightRows == null) continue;

            for (int leftRow : leftEntry.getValue()) {
                for (int rightRow : rightRows) {
                    // Verify key equality (hash collision check)
                    if (!RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                        continue;
                    }

                    int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                    int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);
                    int wOut = Math.multiplyExact(w1, w2);
                    if (wOut == 0) continue;

                    // Copy key columns from left
                    for (int i = 0; i < keyColumnIndices.length; i++) {
                        ArrowUtils.setValue(result.getVector(i), outRow,
                                ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                    }
                    // Compute combined value columns
                    Object[] combinedValues = valueCombiner.combine(this.data, leftRow, other.data, rightRow);
                    for (int i = 0; i < combinedValues.length; i++) {
                        ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, combinedValues[i]);
                    }
                    ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, wOut);
                    outRow++;
                }
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, outKeyIndices);
        return new IndexedZSet(allocator, keySchema, outputValueSchema, outKeyIndices, outValueIndices,
                outFull, result, outHashes);
    }

    /**
     * Left outer join: matched rows + unmatched left rows.
     * For unmatched left rows, {@code unmatchedLeftMapper} produces the output value columns.
     */
    public IndexedZSet leftJoin(IndexedZSet other, Schema outputValueSchema,
                                 RowCombiner valueCombiner, RowMapper unmatchedLeftMapper) {
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        List<org.apache.arrow.vector.types.pojo.Field> outFields = new ArrayList<>();
        outFields.addAll(keySchema.getFields());
        outFields.addAll(outputValueSchema.getFields());
        outFields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema outFull = new Schema(outFields);
        int[] outKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < outKeyIndices.length; i++) outKeyIndices[i] = i;
        int[] outValueIndices = new int[outputValueSchema.getFields().size()];
        for (int i = 0; i < outValueIndices.length; i++) outValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int outWeightCol = outFields.size() - 1;
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int otherWeightCol = other.data.getFieldVectors().size() - 1;
        int outRow = 0;

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());

            for (int leftRow : leftEntry.getValue()) {
                boolean matched = false;
                if (rightRows != null) {
                    for (int rightRow : rightRows) {
                        if (!RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                            continue;
                        }
                        matched = true;
                        int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                        int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);
                        int wOut = Math.multiplyExact(w1, w2);
                        if (wOut == 0) continue;

                        for (int i = 0; i < keyColumnIndices.length; i++) {
                            ArrowUtils.setValue(result.getVector(i), outRow,
                                    ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                        }
                        Object[] vals = valueCombiner.combine(this.data, leftRow, other.data, rightRow);
                        for (int i = 0; i < vals.length; i++) {
                            ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, vals[i]);
                        }
                        ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, wOut);
                        outRow++;
                    }
                }
                if (!matched) {
                    int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                    for (int i = 0; i < keyColumnIndices.length; i++) {
                        ArrowUtils.setValue(result.getVector(i), outRow,
                                ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                    }
                    Object[] unmatchedVals = unmatchedLeftMapper.map(this.data, leftRow);
                    for (int i = 0; i < unmatchedVals.length; i++) {
                        ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, unmatchedVals[i]);
                    }
                    ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, w1);
                    outRow++;
                }
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, outKeyIndices);
        return new IndexedZSet(allocator, keySchema, outputValueSchema, outKeyIndices, outValueIndices,
                outFull, result, outHashes);
    }

    /**
     * Right outer join: matched rows + unmatched right rows.
     * For unmatched right rows, {@code unmatchedRightMapper} produces the output value columns.
     */
    public IndexedZSet rightJoin(IndexedZSet other, Schema outputValueSchema,
                                  RowCombiner valueCombiner, RowMapper unmatchedRightMapper) {
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        List<org.apache.arrow.vector.types.pojo.Field> outFields = new ArrayList<>();
        outFields.addAll(keySchema.getFields());
        outFields.addAll(outputValueSchema.getFields());
        outFields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema outFull = new Schema(outFields);
        int[] outKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < outKeyIndices.length; i++) outKeyIndices[i] = i;
        int[] outValueIndices = new int[outputValueSchema.getFields().size()];
        for (int i = 0; i < outValueIndices.length; i++) outValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int outWeightCol = outFields.size() - 1;
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int otherWeightCol = other.data.getFieldVectors().size() - 1;
        int outRow = 0;

        Set<Integer> matchedRightRows = new HashSet<>();

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());
            if (rightRows == null) continue;

            for (int leftRow : leftEntry.getValue()) {
                for (int rightRow : rightRows) {
                    if (!RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                        continue;
                    }
                    matchedRightRows.add(rightRow);

                    int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                    int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);
                    int wOut = Math.multiplyExact(w1, w2);
                    if (wOut == 0) continue;

                    for (int i = 0; i < keyColumnIndices.length; i++) {
                        ArrowUtils.setValue(result.getVector(i), outRow,
                                ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                    }
                    Object[] vals = valueCombiner.combine(this.data, leftRow, other.data, rightRow);
                    for (int i = 0; i < vals.length; i++) {
                        ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, vals[i]);
                    }
                    ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, wOut);
                    outRow++;
                }
            }
        }

        for (List<Integer> rightRows : rightGroups.values()) {
            for (int rightRow : rightRows) {
                if (matchedRightRows.contains(rightRow)) continue;
                int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);

                for (int i = 0; i < keyColumnIndices.length; i++) {
                    ArrowUtils.setValue(result.getVector(i), outRow,
                            ArrowUtils.getValue(other.data.getVector(other.keyColumnIndices[i]), rightRow));
                }
                Object[] unmatchedVals = unmatchedRightMapper.map(other.data, rightRow);
                for (int i = 0; i < unmatchedVals.length; i++) {
                    ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, unmatchedVals[i]);
                }
                ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, w2);
                outRow++;
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, outKeyIndices);
        return new IndexedZSet(allocator, keySchema, outputValueSchema, outKeyIndices, outValueIndices,
                outFull, result, outHashes);
    }

    /**
     * Full outer join: matched rows + unmatched left rows + unmatched right rows.
     * {@code unmatchedLeftMapper} produces output columns for unmatched left rows.
     * {@code unmatchedRightMapper} produces output columns for unmatched right rows.
     */
    public IndexedZSet fullJoin(IndexedZSet other, Schema outputValueSchema,
                                 RowCombiner valueCombiner,
                                 RowMapper unmatchedLeftMapper, RowMapper unmatchedRightMapper) {
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        List<org.apache.arrow.vector.types.pojo.Field> outFields = new ArrayList<>();
        outFields.addAll(keySchema.getFields());
        outFields.addAll(outputValueSchema.getFields());
        outFields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema outFull = new Schema(outFields);
        int[] outKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < outKeyIndices.length; i++) outKeyIndices[i] = i;
        int[] outValueIndices = new int[outputValueSchema.getFields().size()];
        for (int i = 0; i < outValueIndices.length; i++) outValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int outWeightCol = outFields.size() - 1;
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int otherWeightCol = other.data.getFieldVectors().size() - 1;
        int outRow = 0;

        Set<Integer> matchedRightRows = new HashSet<>();

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());

            for (int leftRow : leftEntry.getValue()) {
                boolean matched = false;
                if (rightRows != null) {
                    for (int rightRow : rightRows) {
                        if (!RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                            continue;
                        }
                        matched = true;
                        matchedRightRows.add(rightRow);

                        int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                        int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);
                        int wOut = Math.multiplyExact(w1, w2);
                        if (wOut == 0) continue;

                        for (int i = 0; i < keyColumnIndices.length; i++) {
                            ArrowUtils.setValue(result.getVector(i), outRow,
                                    ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                        }
                        Object[] vals = valueCombiner.combine(this.data, leftRow, other.data, rightRow);
                        for (int i = 0; i < vals.length; i++) {
                            ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, vals[i]);
                        }
                        ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, wOut);
                        outRow++;
                    }
                }
                if (!matched) {
                    int w1 = ((IntVector) this.data.getVector(thisWeightCol)).get(leftRow);
                    for (int i = 0; i < keyColumnIndices.length; i++) {
                        ArrowUtils.setValue(result.getVector(i), outRow,
                                ArrowUtils.getValue(this.data.getVector(keyColumnIndices[i]), leftRow));
                    }
                    Object[] unmatchedVals = unmatchedLeftMapper.map(this.data, leftRow);
                    for (int i = 0; i < unmatchedVals.length; i++) {
                        ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, unmatchedVals[i]);
                    }
                    ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, w1);
                    outRow++;
                }
            }
        }

        for (List<Integer> rightRows : rightGroups.values()) {
            for (int rightRow : rightRows) {
                if (matchedRightRows.contains(rightRow)) continue;
                int w2 = ((IntVector) other.data.getVector(otherWeightCol)).get(rightRow);

                for (int i = 0; i < keyColumnIndices.length; i++) {
                    ArrowUtils.setValue(result.getVector(i), outRow,
                            ArrowUtils.getValue(other.data.getVector(other.keyColumnIndices[i]), rightRow));
                }
                Object[] unmatchedVals = unmatchedRightMapper.map(other.data, rightRow);
                for (int i = 0; i < unmatchedVals.length; i++) {
                    ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, unmatchedVals[i]);
                }
                ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, w2);
                outRow++;
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, outKeyIndices);
        return new IndexedZSet(allocator, keySchema, outputValueSchema, outKeyIndices, outValueIndices,
                outFull, result, outHashes);
    }

    /**
     * Semi-join: return left rows that have at least one matching key on the right.
     * Output schema matches the left side. Each matching left row keeps its original weight.
     */
    public IndexedZSet semiJoin(IndexedZSet other) {
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int outRow = 0;

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());
            if (rightRows == null) continue;

            for (int leftRow : leftEntry.getValue()) {
                boolean matched = false;
                for (int rightRow : rightRows) {
                    if (RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                        matched = true;
                        break;
                    }
                }
                if (matched) {
                    ArrowUtils.copyRow(this.data, leftRow, result, outRow);
                    outRow++;
                }
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, keyColumnIndices);
        return new IndexedZSet(allocator, keySchema, valueSchema, keyColumnIndices, valueColumnIndices,
                fullSchema, result, outHashes);
    }

    /**
     * Anti-join: return left rows that have NO matching key on the right.
     * Output schema matches the left side. Each unmatched left row keeps its original weight.
     */
    public IndexedZSet antiJoin(IndexedZSet other) {
        Map<Integer, List<Integer>> leftGroups = buildKeyGroups();
        Map<Integer, List<Integer>> rightGroups = other.buildKeyGroups();

        VectorSchemaRoot result = VectorSchemaRoot.create(fullSchema, allocator);
        result.allocateNew();
        int outRow = 0;

        for (Map.Entry<Integer, List<Integer>> leftEntry : leftGroups.entrySet()) {
            List<Integer> rightRows = rightGroups.get(leftEntry.getKey());

            for (int leftRow : leftEntry.getValue()) {
                boolean matched = false;
                if (rightRows != null) {
                    for (int rightRow : rightRows) {
                        if (RowHasher.rowsEqual(this.data, leftRow, other.data, rightRow, keyColumnIndices)) {
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched) {
                    ArrowUtils.copyRow(this.data, leftRow, result, outRow);
                    outRow++;
                }
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, keyColumnIndices);
        return new IndexedZSet(allocator, keySchema, valueSchema, keyColumnIndices, valueColumnIndices,
                fullSchema, result, outHashes);
    }

    // ---- Aggregate ----

    /**
     * Per-key aggregation. Produces an IndexedZSet where each key maps to a single aggregate result with weight 1.
     */
    public <R, IR> IndexedZSet aggregate(AggregateDescription<R, IR> agg,
                                          Schema resultValueSchema,
                                          java.util.function.Function<R, Object[]> resultToRow) {
        return aggregateImpl(null, agg, resultValueSchema, resultToRow);
    }

    /**
     * Per-key aggregation for a subset of groups identified by key hash.
     * Only processes groups whose key hash is in {@code affectedKeyHashes}.
     * Groups with non-matching hashes are skipped entirely.
     */
    public <R, IR> IndexedZSet aggregateForKeys(Set<Integer> affectedKeyHashes,
                                                 AggregateDescription<R, IR> agg,
                                                 Schema resultValueSchema,
                                                 java.util.function.Function<R, Object[]> resultToRow) {
        return aggregateImpl(affectedKeyHashes, agg, resultValueSchema, resultToRow);
    }

    private <R, IR> IndexedZSet aggregateImpl(Set<Integer> affectedKeyHashes,
                                               AggregateDescription<R, IR> agg,
                                               Schema resultValueSchema,
                                               java.util.function.Function<R, Object[]> resultToRow) {
        Map<Integer, List<Integer>> groups = buildKeyGroups();

        List<org.apache.arrow.vector.types.pojo.Field> outFields = new ArrayList<>();
        outFields.addAll(keySchema.getFields());
        outFields.addAll(resultValueSchema.getFields());
        outFields.add(new org.apache.arrow.vector.types.pojo.Field(ArrowUtils.WEIGHT_COLUMN,
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null));
        Schema outFull = new Schema(outFields);
        int[] outKeyIndices = new int[keyColumnIndices.length];
        for (int i = 0; i < outKeyIndices.length; i++) outKeyIndices[i] = i;
        int[] outValueIndices = new int[resultValueSchema.getFields().size()];
        for (int i = 0; i < outValueIndices.length; i++) outValueIndices[i] = keyColumnIndices.length + i;

        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int outWeightCol = outFields.size() - 1;
        int thisWeightCol = this.data.getFieldVectors().size() - 1;
        int outRow = 0;

        for (Map.Entry<Integer, List<Integer>> entry : groups.entrySet()) {
            // Skip groups not in the affected set (when filtering)
            if (affectedKeyHashes != null && !affectedKeyHashes.contains(entry.getKey())) continue;

            List<Integer> rows = entry.getValue();
            // Sub-group by actual key equality (handle hash collisions)
            List<List<Integer>> subGroups = subGroupByKey(rows);

            for (List<Integer> subGroup : subGroups) {
                IR acc = agg.initialValue();
                int valCols = valueColumnIndices.length;
                for (int row : subGroup) {
                    Object[] values = new Object[valCols];
                    for (int i = 0; i < valCols; i++) {
                        values[i] = ArrowUtils.getValue(data.getVector(valueColumnIndices[i]), row);
                    }
                    int weight = ((IntVector) data.getVector(thisWeightCol)).get(row);
                    acc = agg.accumulator().accumulate(acc, values, weight);
                }
                R aggResult = agg.finalizer().apply(acc);
                Object[] resultValues = resultToRow.apply(aggResult);

                // Copy key from first row of the group
                int keyRow = subGroup.get(0);
                for (int i = 0; i < keyColumnIndices.length; i++) {
                    ArrowUtils.setValue(result.getVector(i), outRow,
                            ArrowUtils.getValue(data.getVector(keyColumnIndices[i]), keyRow));
                }
                for (int i = 0; i < resultValues.length; i++) {
                    ArrowUtils.setValue(result.getVector(keyColumnIndices.length + i), outRow, resultValues[i]);
                }
                ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, 1);
                outRow++;
            }
        }
        result.setRowCount(outRow);

        int[] outHashes = computeKeyHashes(result, outKeyIndices);
        return new IndexedZSet(allocator, keySchema, resultValueSchema, outKeyIndices, outValueIndices,
                outFull, result, outHashes);
    }

    // ---- Flatten / Deindex ----

    /**
     * Flatten back to ZSet by combining key and value columns.
     */
    public ZSet flatten(Schema outputDataSchema, RowMapper mapper) {
        Schema outFull = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int weightCol = data.getFieldVectors().size() - 1;
        int outWeightCol = result.getFieldVectors().size() - 1;

        for (int row = 0; row < data.getRowCount(); row++) {
            Object[] outValues = mapper.map(data, row);
            for (int col = 0; col < outValues.length; col++) {
                ArrowUtils.setValue(result.getVector(col), row, outValues[col]);
            }
            int weight = ((IntVector) data.getVector(weightCol)).get(row);
            ((IntVector) result.getVector(outWeightCol)).setSafe(row, weight);
        }
        result.setRowCount(data.getRowCount());

        return ZSet.fromRoot(outputDataSchema, result, allocator);
    }

    /**
     * Deindex: flatten back to ZSet keeping only value columns + weight.
     */
    public ZSet deindex() {
        Schema outFull = ArrowUtils.createSchemaWithWeight(valueSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, allocator);
        result.allocateNew();
        int weightCol = data.getFieldVectors().size() - 1;
        int outWeightCol = result.getFieldVectors().size() - 1;

        for (int row = 0; row < data.getRowCount(); row++) {
            for (int i = 0; i < valueColumnIndices.length; i++) {
                ArrowUtils.setValue(result.getVector(i), row,
                        ArrowUtils.getValue(data.getVector(valueColumnIndices[i]), row));
            }
            int weight = ((IntVector) data.getVector(weightCol)).get(row);
            ((IntVector) result.getVector(outWeightCol)).setSafe(row, weight);
        }
        result.setRowCount(data.getRowCount());
        return ZSet.fromRoot(valueSchema, result, allocator);
    }

    /**
     * Count distinct keys.
     */
    public int groupCount() {
        Map<Integer, List<Integer>> groups = buildKeyGroups();
        int count = 0;
        for (List<Integer> rows : groups.values()) {
            count += subGroupByKey(rows).size();
        }
        return count;
    }

    // ---- Internal ----

    private static int[] computeKeyHashes(VectorSchemaRoot data, int[] keyColumnIndices) {
        int rowCount = data.getRowCount();
        int[] hashes = new int[rowCount];
        for (int row = 0; row < rowCount; row++) {
            hashes[row] = RowHasher.hashRow(data, row, keyColumnIndices);
        }
        return hashes;
    }

    private Map<Integer, List<Integer>> buildKeyGroups() {
        Map<Integer, List<Integer>> groups = new HashMap<>();
        for (int row = 0; row < data.getRowCount(); row++) {
            groups.computeIfAbsent(rowKeyHashes[row], k -> new ArrayList<>(4)).add(row);
        }
        return groups;
    }

    private List<List<Integer>> subGroupByKey(List<Integer> rows) {
        List<List<Integer>> subGroups = new ArrayList<>();
        for (int row : rows) {
            boolean found = false;
            for (List<Integer> sg : subGroups) {
                if (RowHasher.rowsEqual(data, sg.get(0), data, row, keyColumnIndices)) {
                    sg.add(row);
                    found = true;
                    break;
                }
            }
            if (!found) {
                List<Integer> sg = new ArrayList<>(4);
                sg.add(row);
                subGroups.add(sg);
            }
        }
        return subGroups;
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
        if (data == null) return "IndexedZSet(closed)";
        StringBuilder sb = new StringBuilder("IndexedZSet{");
        int weightCol = data.getFieldVectors().size() - 1;
        for (int row = 0; row < data.getRowCount(); row++) {
            if (row > 0) sb.append(", ");
            sb.append("(");
            for (int i = 0; i < keyColumnIndices.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(ArrowUtils.getValue(data.getVector(keyColumnIndices[i]), row));
            }
            sb.append(")=>(");
            for (int i = 0; i < valueColumnIndices.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(ArrowUtils.getValue(data.getVector(valueColumnIndices[i]), row));
            }
            sb.append(")=>").append(((IntVector) data.getVector(weightCol)).get(row));
        }
        sb.append("}");
        return sb.toString();
    }
}
