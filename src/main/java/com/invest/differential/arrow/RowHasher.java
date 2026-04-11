package com.invest.differential.arrow;

import org.apache.arrow.vector.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Computes hash codes and equality checks over Arrow vector rows.
 * Used for ZSet compaction (merging duplicate rows).
 */
public final class RowHasher {

    private RowHasher() {}

    public static int hashRow(VectorSchemaRoot root, int rowIndex, int[] columns) {
        int hash = 1;
        for (int col : columns) {
            hash = 31 * hash + hashValue(root.getVector(col), rowIndex);
        }
        return hash;
    }

    public static boolean rowsEqual(VectorSchemaRoot r1, int row1,
                                     VectorSchemaRoot r2, int row2,
                                     int[] columns) {
        for (int col : columns) {
            if (!valuesEqual(r1.getVector(col), row1, r2.getVector(col), row2)) {
                return false;
            }
        }
        return true;
    }

    private static int hashValue(FieldVector vector, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return 0;
        }
        if (vector instanceof IntVector v) {
            return Integer.hashCode(v.get(rowIndex));
        } else if (vector instanceof BigIntVector v) {
            return Long.hashCode(v.get(rowIndex));
        } else if (vector instanceof Float4Vector v) {
            return Float.hashCode(v.get(rowIndex));
        } else if (vector instanceof Float8Vector v) {
            return Double.hashCode(v.get(rowIndex));
        } else if (vector instanceof VarCharVector v) {
            return Arrays.hashCode(v.get(rowIndex));
        } else if (vector instanceof BitVector v) {
            return Integer.hashCode(v.get(rowIndex));
        } else if (vector instanceof SmallIntVector v) {
            return Short.hashCode(v.get(rowIndex));
        } else if (vector instanceof TinyIntVector v) {
            return Byte.hashCode(v.get(rowIndex));
        } else if (vector instanceof VarBinaryVector v) {
            return Arrays.hashCode(v.get(rowIndex));
        } else if (vector instanceof DateDayVector v) {
            return Integer.hashCode(v.get(rowIndex));
        } else if (vector instanceof DateMilliVector v) {
            return Long.hashCode(v.get(rowIndex));
        } else if (vector instanceof TimeStampMilliVector v) {
            return Long.hashCode(v.get(rowIndex));
        } else if (vector instanceof TimeStampMicroVector v) {
            return Long.hashCode(v.get(rowIndex));
        } else if (vector instanceof DecimalVector v) {
            return v.getObject(rowIndex).hashCode();
        }
        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    private static boolean valuesEqual(FieldVector v1, int row1, FieldVector v2, int row2) {
        boolean null1 = v1.isNull(row1);
        boolean null2 = v2.isNull(row2);
        if (null1 && null2) return true;
        if (null1 || null2) return false;

        if (v1 instanceof IntVector a && v2 instanceof IntVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof BigIntVector a && v2 instanceof BigIntVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof Float4Vector a && v2 instanceof Float4Vector b) {
            return Float.compare(a.get(row1), b.get(row2)) == 0;
        } else if (v1 instanceof Float8Vector a && v2 instanceof Float8Vector b) {
            return Double.compare(a.get(row1), b.get(row2)) == 0;
        } else if (v1 instanceof VarCharVector a && v2 instanceof VarCharVector b) {
            return Arrays.equals(a.get(row1), b.get(row2));
        } else if (v1 instanceof BitVector a && v2 instanceof BitVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof SmallIntVector a && v2 instanceof SmallIntVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof TinyIntVector a && v2 instanceof TinyIntVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof VarBinaryVector a && v2 instanceof VarBinaryVector b) {
            return Arrays.equals(a.get(row1), b.get(row2));
        } else if (v1 instanceof DateDayVector a && v2 instanceof DateDayVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof DateMilliVector a && v2 instanceof DateMilliVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof TimeStampMilliVector a && v2 instanceof TimeStampMilliVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof TimeStampMicroVector a && v2 instanceof TimeStampMicroVector b) {
            return a.get(row1) == b.get(row2);
        } else if (v1 instanceof DecimalVector a && v2 instanceof DecimalVector b) {
            return a.getObject(row1).equals(b.getObject(row2));
        }
        // Fallback: use generic getValue comparison
        Object val1 = ArrowUtils.getValue(v1, row1);
        Object val2 = ArrowUtils.getValue(v2, row2);
        return java.util.Objects.equals(val1, val2);
    }
}
