package com.invest.differential.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for Arrow memory management, schema manipulation, and vector operations.
 */
public final class ArrowUtils {

    public static final String WEIGHT_COLUMN = "__weight__";

    private ArrowUtils() {}

    public static Schema createSchemaWithWeight(Schema dataSchema) {
        List<Field> fields = new ArrayList<>(dataSchema.getFields());
        fields.add(new Field(WEIGHT_COLUMN, FieldType.notNullable(new ArrowType.Int(32, true)), null));
        return new Schema(fields);
    }

    public static Schema stripWeightColumn(Schema schemaWithWeight) {
        List<Field> fields = new ArrayList<>();
        for (Field f : schemaWithWeight.getFields()) {
            if (!f.getName().equals(WEIGHT_COLUMN)) {
                fields.add(f);
            }
        }
        return new Schema(fields);
    }

    public static int getWeightColumnIndex(VectorSchemaRoot root) {
        return root.getSchema().getFields().size() - 1;
    }

    public static VectorSchemaRoot createEmpty(Schema schemaWithWeight, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithWeight, allocator);
        root.allocateNew();
        root.setRowCount(0);
        return root;
    }

    public static Object getValue(FieldVector vector, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return null;
        }
        if (vector instanceof IntVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof BigIntVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof Float4Vector v) {
            return v.get(rowIndex);
        } else if (vector instanceof Float8Vector v) {
            return v.get(rowIndex);
        } else if (vector instanceof VarCharVector v) {
            return new String(v.get(rowIndex), StandardCharsets.UTF_8);
        } else if (vector instanceof BitVector v) {
            return v.get(rowIndex) != 0;
        } else if (vector instanceof SmallIntVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof TinyIntVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof DecimalVector v) {
            return v.getObject(rowIndex);
        } else if (vector instanceof Decimal256Vector v) {
            return v.getObject(rowIndex);
        } else if (vector instanceof VarBinaryVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof DateDayVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof DateMilliVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof TimeStampMilliVector v) {
            return v.get(rowIndex);
        } else if (vector instanceof TimeStampMicroVector v) {
            return v.get(rowIndex);
        }
        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    public static void setValue(FieldVector vector, int rowIndex, Object value) {
        if (value == null) {
            vector.setNull(rowIndex);
            return;
        }
        if (vector instanceof IntVector v) {
            v.setSafe(rowIndex, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector v) {
            v.setSafe(rowIndex, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector v) {
            v.setSafe(rowIndex, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector v) {
            v.setSafe(rowIndex, ((Number) value).doubleValue());
        } else if (vector instanceof VarCharVector v) {
            byte[] bytes = value instanceof byte[] ? (byte[]) value : value.toString().getBytes(StandardCharsets.UTF_8);
            v.setSafe(rowIndex, bytes);
        } else if (vector instanceof BitVector v) {
            v.setSafe(rowIndex, value instanceof Boolean b ? (b ? 1 : 0) : ((Number) value).intValue());
        } else if (vector instanceof SmallIntVector v) {
            v.setSafe(rowIndex, ((Number) value).shortValue());
        } else if (vector instanceof TinyIntVector v) {
            v.setSafe(rowIndex, ((Number) value).byteValue());
        } else if (vector instanceof VarBinaryVector v) {
            v.setSafe(rowIndex, (byte[]) value);
        } else if (vector instanceof DateDayVector v) {
            v.setSafe(rowIndex, ((Number) value).intValue());
        } else if (vector instanceof DateMilliVector v) {
            v.setSafe(rowIndex, ((Number) value).longValue());
        } else if (vector instanceof TimeStampMilliVector v) {
            v.setSafe(rowIndex, ((Number) value).longValue());
        } else if (vector instanceof TimeStampMicroVector v) {
            v.setSafe(rowIndex, ((Number) value).longValue());
        } else {
            throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
        }
    }

    public static void copyRow(VectorSchemaRoot src, int srcRow, VectorSchemaRoot dst, int dstRow) {
        for (int col = 0; col < src.getFieldVectors().size(); col++) {
            FieldVector srcVec = src.getVector(col);
            FieldVector dstVec = dst.getVector(col);
            setValue(dstVec, dstRow, getValue(srcVec, srcRow));
        }
    }

    public static void copyRowColumns(VectorSchemaRoot src, int srcRow,
                                       VectorSchemaRoot dst, int dstRow,
                                       int[] srcColumns, int dstColOffset) {
        for (int i = 0; i < srcColumns.length; i++) {
            FieldVector srcVec = src.getVector(srcColumns[i]);
            FieldVector dstVec = dst.getVector(dstColOffset + i);
            setValue(dstVec, dstRow, getValue(srcVec, srcRow));
        }
    }

    public static VectorSchemaRoot cloneRoot(VectorSchemaRoot src, BufferAllocator allocator) {
        Schema schema = src.getSchema();
        VectorSchemaRoot dst = VectorSchemaRoot.create(schema, allocator);
        dst.allocateNew();
        int rowCount = src.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            copyRow(src, row, dst, row);
        }
        dst.setRowCount(rowCount);
        return dst;
    }

    public static int[] dataColumnIndices(VectorSchemaRoot root) {
        int total = root.getFieldVectors().size();
        int[] indices = new int[total - 1]; // exclude weight column
        for (int i = 0; i < total - 1; i++) {
            indices[i] = i;
        }
        return indices;
    }

    public static Schema subSchema(Schema schema, int[] columnIndices) {
        List<Field> fields = schema.getFields();
        List<Field> sub = new ArrayList<>(columnIndices.length);
        for (int idx : columnIndices) {
            sub.add(fields.get(idx));
        }
        return new Schema(sub);
    }
}
