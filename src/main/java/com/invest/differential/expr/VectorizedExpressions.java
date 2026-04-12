package com.invest.differential.expr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Factory for vectorized expression evaluators that operate on entire Arrow batches.
 * Avoids per-row Object boxing/unboxing for significant performance gains.
 */
public final class VectorizedExpressions {

    private VectorizedExpressions() {}

    // ── Leaf evaluators ──

    /** Returns a copy of the column at the given index. */
    public static VectorizedEvaluator fieldRef(int index) {
        return (batch, alloc) -> {
            FieldVector src = batch.getVector(index);
            TransferPair tp = src.getTransferPair(alloc);
            tp.splitAndTransfer(0, src.getValueCount());
            return (FieldVector) tp.getTo();
        };
    }

    /** Creates a constant vector filled with the given int value. */
    public static VectorizedEvaluator intLiteral(int value) {
        return (batch, alloc) -> {
            int count = batch.getRowCount();
            IntVector v = new IntVector("lit", alloc);
            v.allocateNew(count);
            for (int i = 0; i < count; i++) v.set(i, value);
            v.setValueCount(count);
            return v;
        };
    }

    /** Creates a constant vector filled with the given long value. */
    public static VectorizedEvaluator longLiteral(long value) {
        return (batch, alloc) -> {
            int count = batch.getRowCount();
            BigIntVector v = new BigIntVector("lit", alloc);
            v.allocateNew(count);
            for (int i = 0; i < count; i++) v.set(i, value);
            v.setValueCount(count);
            return v;
        };
    }

    /** Creates a constant vector filled with the given double value. */
    public static VectorizedEvaluator doubleLiteral(double value) {
        return (batch, alloc) -> {
            int count = batch.getRowCount();
            Float8Vector v = new Float8Vector("lit", alloc);
            v.allocateNew(count);
            for (int i = 0; i < count; i++) v.set(i, value);
            v.setValueCount(count);
            return v;
        };
    }

    /** Creates a constant string vector. */
    public static VectorizedEvaluator stringLiteral(String value) {
        return (batch, alloc) -> {
            int count = batch.getRowCount();
            VarCharVector v = new VarCharVector("lit", alloc);
            v.allocateNew(count);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < count; i++) v.set(i, bytes);
            v.setValueCount(count);
            return v;
        };
    }

    /** Creates an all-null vector of the given Arrow type. */
    public static VectorizedEvaluator nullLiteral(ArrowType type) {
        return (batch, alloc) -> {
            int count = batch.getRowCount();
            FieldVector v = FieldType.nullable(type).createNewSingleVector("lit", alloc, null);
            v.allocateNew();
            for (int i = 0; i < count; i++) v.setNull(i);
            v.setValueCount(count);
            return v;
        };
    }

    // ── Arithmetic evaluators ──

    public static VectorizedEvaluator add(VectorizedEvaluator left, VectorizedEvaluator right) {
        return numericBinaryOp(left, right, NumOp.ADD);
    }

    public static VectorizedEvaluator subtract(VectorizedEvaluator left, VectorizedEvaluator right) {
        return numericBinaryOp(left, right, NumOp.SUB);
    }

    public static VectorizedEvaluator multiply(VectorizedEvaluator left, VectorizedEvaluator right) {
        return numericBinaryOp(left, right, NumOp.MUL);
    }

    public static VectorizedEvaluator divide(VectorizedEvaluator left, VectorizedEvaluator right) {
        return numericBinaryOp(left, right, NumOp.DIV);
    }

    public static VectorizedEvaluator negate(VectorizedEvaluator operand) {
        return (batch, alloc) -> {
            try (FieldVector v = operand.evaluate(batch, alloc)) {
                int count = v.getValueCount();
                if (v instanceof IntVector iv) {
                    IntVector result = new IntVector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (iv.isNull(i)) result.setNull(i);
                        else result.set(i, -iv.get(i));
                    }
                    result.setValueCount(count);
                    return result;
                } else if (v instanceof BigIntVector iv) {
                    BigIntVector result = new BigIntVector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (iv.isNull(i)) result.setNull(i);
                        else result.set(i, -iv.get(i));
                    }
                    result.setValueCount(count);
                    return result;
                } else if (v instanceof Float8Vector fv) {
                    Float8Vector result = new Float8Vector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (fv.isNull(i)) result.setNull(i);
                        else result.set(i, -fv.get(i));
                    }
                    result.setValueCount(count);
                    return result;
                }
                throw new UnsupportedOperationException("negate unsupported for " + v.getClass().getSimpleName());
            }
        };
    }

    // ── Comparison evaluators ──

    public static VectorizedEvaluator gt(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.GT);
    }

    public static VectorizedEvaluator gte(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.GTE);
    }

    public static VectorizedEvaluator lt(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.LT);
    }

    public static VectorizedEvaluator lte(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.LTE);
    }

    public static VectorizedEvaluator equal(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.EQ);
    }

    public static VectorizedEvaluator notEqual(VectorizedEvaluator left, VectorizedEvaluator right) {
        return comparisonOp(left, right, CmpOp.NEQ);
    }

    // ── Boolean evaluators ──

    public static VectorizedEvaluator and(VectorizedEvaluator left, VectorizedEvaluator right) {
        return (batch, alloc) -> {
            try (FieldVector lv = left.evaluate(batch, alloc);
                 FieldVector rv = right.evaluate(batch, alloc)) {
                BitVector lb = toBitVector(lv);
                BitVector rb = toBitVector(rv);
                int count = lb.getValueCount();
                BitVector result = new BitVector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (lb.isNull(i) || rb.isNull(i)) result.setNull(i);
                    else result.set(i, (lb.get(i) & rb.get(i)));
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    public static VectorizedEvaluator or(VectorizedEvaluator left, VectorizedEvaluator right) {
        return (batch, alloc) -> {
            try (FieldVector lv = left.evaluate(batch, alloc);
                 FieldVector rv = right.evaluate(batch, alloc)) {
                BitVector lb = toBitVector(lv);
                BitVector rb = toBitVector(rv);
                int count = lb.getValueCount();
                BitVector result = new BitVector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (lb.isNull(i) || rb.isNull(i)) result.setNull(i);
                    else result.set(i, (lb.get(i) | rb.get(i)));
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    public static VectorizedEvaluator not(VectorizedEvaluator operand) {
        return (batch, alloc) -> {
            try (FieldVector v = operand.evaluate(batch, alloc)) {
                BitVector bv = toBitVector(v);
                int count = bv.getValueCount();
                BitVector result = new BitVector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (bv.isNull(i)) result.setNull(i);
                    else result.set(i, bv.get(i) == 0 ? 1 : 0);
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    // ── String evaluators ──

    public static VectorizedEvaluator upper(VectorizedEvaluator operand) {
        return stringUnaryOp(operand, String::toUpperCase);
    }

    public static VectorizedEvaluator lower(VectorizedEvaluator operand) {
        return stringUnaryOp(operand, String::toLowerCase);
    }

    public static VectorizedEvaluator charLength(VectorizedEvaluator operand) {
        return (batch, alloc) -> {
            try (FieldVector v = operand.evaluate(batch, alloc)) {
                VarCharVector sv = (VarCharVector) v;
                int count = sv.getValueCount();
                IntVector result = new IntVector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (sv.isNull(i)) result.setNull(i);
                    else result.set(i, new String(sv.get(i), StandardCharsets.UTF_8).length());
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    // ── Batch filter utility ──

    /**
     * Filters a VectorSchemaRoot using a vectorized boolean predicate.
     * Returns a new VectorSchemaRoot containing only matching rows.
     * Caller owns the returned root.
     */
    public static VectorSchemaRoot filterBatch(VectorSchemaRoot batch,
                                                VectorizedEvaluator predicate,
                                                BufferAllocator allocator) {
        try (FieldVector mask = predicate.evaluate(batch, allocator)) {
            BitVector bits = toBitVector(mask);
            int count = batch.getRowCount();

            // Count matching rows
            int matchCount = 0;
            for (int i = 0; i < count; i++) {
                if (!bits.isNull(i) && bits.get(i) != 0) matchCount++;
            }

            VectorSchemaRoot result = VectorSchemaRoot.create(batch.getSchema(), allocator);
            result.allocateNew();
            int outRow = 0;
            for (int i = 0; i < count; i++) {
                if (!bits.isNull(i) && bits.get(i) != 0) {
                    for (int col = 0; col < batch.getFieldVectors().size(); col++) {
                        FieldVector src = batch.getVector(col);
                        FieldVector dst = result.getVector(col);
                        copyCell(src, i, dst, outRow);
                    }
                    outRow++;
                }
            }
            result.setRowCount(matchCount);
            return result;
        }
    }

    /**
     * Projects a VectorSchemaRoot through a list of vectorized evaluators.
     * Each evaluator produces one output column.
     * Returns a new VectorSchemaRoot. Caller owns it.
     */
    public static VectorSchemaRoot projectBatch(VectorSchemaRoot batch,
                                                 List<VectorizedEvaluator> columns,
                                                 BufferAllocator allocator) {
        FieldVector[] vecs = new FieldVector[columns.size()];
        try {
            for (int i = 0; i < columns.size(); i++) {
                vecs[i] = columns.get(i).evaluate(batch, allocator);
            }
            return new VectorSchemaRoot(List.of(vecs));
        } catch (Exception e) {
            for (FieldVector v : vecs) {
                if (v != null) v.close();
            }
            throw e;
        }
    }

    // ── Internal helpers ──

    private enum NumOp { ADD, SUB, MUL, DIV }
    private enum CmpOp { GT, GTE, LT, LTE, EQ, NEQ }

    private static VectorizedEvaluator numericBinaryOp(VectorizedEvaluator left,
                                                        VectorizedEvaluator right,
                                                        NumOp op) {
        return (batch, alloc) -> {
            try (FieldVector lv = left.evaluate(batch, alloc);
                 FieldVector rv = right.evaluate(batch, alloc)) {
                int count = lv.getValueCount();

                if (lv instanceof IntVector li && rv instanceof IntVector ri) {
                    IntVector result = new IntVector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, intOp(li.get(i), ri.get(i), op));
                    }
                    result.setValueCount(count);
                    return result;
                }

                if (lv instanceof BigIntVector li && rv instanceof BigIntVector ri) {
                    BigIntVector result = new BigIntVector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, longOp(li.get(i), ri.get(i), op));
                    }
                    result.setValueCount(count);
                    return result;
                }

                if (lv instanceof Float8Vector li && rv instanceof Float8Vector ri) {
                    Float8Vector result = new Float8Vector("result", alloc);
                    result.allocateNew(count);
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, doubleOp(li.get(i), ri.get(i), op));
                    }
                    result.setValueCount(count);
                    return result;
                }

                // Mixed types: promote to double
                Float8Vector result = new Float8Vector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (lv.isNull(i) || rv.isNull(i)) result.setNull(i);
                    else result.set(i, doubleOp(toDouble(lv, i), toDouble(rv, i), op));
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    private static VectorizedEvaluator comparisonOp(VectorizedEvaluator left,
                                                     VectorizedEvaluator right,
                                                     CmpOp op) {
        return (batch, alloc) -> {
            try (FieldVector lv = left.evaluate(batch, alloc);
                 FieldVector rv = right.evaluate(batch, alloc)) {
                int count = lv.getValueCount();
                BitVector result = new BitVector("result", alloc);
                result.allocateNew(count);

                if (lv instanceof IntVector li && rv instanceof IntVector ri) {
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, intCmp(li.get(i), ri.get(i), op) ? 1 : 0);
                    }
                } else if (lv instanceof BigIntVector li && rv instanceof BigIntVector ri) {
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, longCmp(li.get(i), ri.get(i), op) ? 1 : 0);
                    }
                } else if (lv instanceof Float8Vector li && rv instanceof Float8Vector ri) {
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else result.set(i, doubleCmp(li.get(i), ri.get(i), op) ? 1 : 0);
                    }
                } else if (lv instanceof VarCharVector li && rv instanceof VarCharVector ri) {
                    for (int i = 0; i < count; i++) {
                        if (li.isNull(i) || ri.isNull(i)) result.setNull(i);
                        else {
                            String ls = new String(li.get(i), StandardCharsets.UTF_8);
                            String rs = new String(ri.get(i), StandardCharsets.UTF_8);
                            result.set(i, stringCmp(ls, rs, op) ? 1 : 0);
                        }
                    }
                } else {
                    // Mixed numeric: promote to double
                    for (int i = 0; i < count; i++) {
                        if (lv.isNull(i) || rv.isNull(i)) result.setNull(i);
                        else result.set(i, doubleCmp(toDouble(lv, i), toDouble(rv, i), op) ? 1 : 0);
                    }
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    private static VectorizedEvaluator stringUnaryOp(VectorizedEvaluator operand,
                                                      java.util.function.UnaryOperator<String> fn) {
        return (batch, alloc) -> {
            try (FieldVector v = operand.evaluate(batch, alloc)) {
                VarCharVector sv = (VarCharVector) v;
                int count = sv.getValueCount();
                VarCharVector result = new VarCharVector("result", alloc);
                result.allocateNew(count);
                for (int i = 0; i < count; i++) {
                    if (sv.isNull(i)) result.setNull(i);
                    else {
                        String s = new String(sv.get(i), StandardCharsets.UTF_8);
                        result.set(i, fn.apply(s).getBytes(StandardCharsets.UTF_8));
                    }
                }
                result.setValueCount(count);
                return result;
            }
        };
    }

    private static int intOp(int a, int b, NumOp op) {
        return switch (op) {
            case ADD -> a + b;
            case SUB -> a - b;
            case MUL -> a * b;
            case DIV -> a / b;
        };
    }

    private static long longOp(long a, long b, NumOp op) {
        return switch (op) {
            case ADD -> a + b;
            case SUB -> a - b;
            case MUL -> a * b;
            case DIV -> a / b;
        };
    }

    private static double doubleOp(double a, double b, NumOp op) {
        return switch (op) {
            case ADD -> a + b;
            case SUB -> a - b;
            case MUL -> a * b;
            case DIV -> a / b;
        };
    }

    private static boolean intCmp(int a, int b, CmpOp op) {
        return switch (op) {
            case GT -> a > b;
            case GTE -> a >= b;
            case LT -> a < b;
            case LTE -> a <= b;
            case EQ -> a == b;
            case NEQ -> a != b;
        };
    }

    private static boolean longCmp(long a, long b, CmpOp op) {
        return switch (op) {
            case GT -> a > b;
            case GTE -> a >= b;
            case LT -> a < b;
            case LTE -> a <= b;
            case EQ -> a == b;
            case NEQ -> a != b;
        };
    }

    private static boolean doubleCmp(double a, double b, CmpOp op) {
        return switch (op) {
            case GT -> a > b;
            case GTE -> a >= b;
            case LT -> a < b;
            case LTE -> a <= b;
            case EQ -> Double.compare(a, b) == 0;
            case NEQ -> Double.compare(a, b) != 0;
        };
    }

    private static boolean stringCmp(String a, String b, CmpOp op) {
        int cmp = a.compareTo(b);
        return switch (op) {
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            case EQ -> cmp == 0;
            case NEQ -> cmp != 0;
        };
    }

    private static double toDouble(FieldVector v, int i) {
        if (v instanceof IntVector iv) return iv.get(i);
        if (v instanceof BigIntVector bv) return bv.get(i);
        if (v instanceof Float4Vector fv) return fv.get(i);
        if (v instanceof Float8Vector dv) return dv.get(i);
        throw new UnsupportedOperationException("Cannot convert " + v.getClass().getSimpleName() + " to double");
    }

    private static BitVector toBitVector(FieldVector v) {
        if (v instanceof BitVector bv) return bv;
        throw new IllegalArgumentException("Expected BitVector, got " + v.getClass().getSimpleName());
    }

    private static void copyCell(FieldVector src, int srcRow, FieldVector dst, int dstRow) {
        if (src.isNull(srcRow)) {
            dst.setNull(dstRow);
            return;
        }
        if (src instanceof IntVector s && dst instanceof IntVector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else if (src instanceof BigIntVector s && dst instanceof BigIntVector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else if (src instanceof Float4Vector s && dst instanceof Float4Vector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else if (src instanceof Float8Vector s && dst instanceof Float8Vector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else if (src instanceof VarCharVector s && dst instanceof VarCharVector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else if (src instanceof BitVector s && dst instanceof BitVector d) {
            d.setSafe(dstRow, s.get(srcRow));
        } else {
            throw new UnsupportedOperationException(
                "copyCell unsupported: " + src.getClass().getSimpleName());
        }
    }
}
