package com.invest.differential.io;

import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.ParquetWriter;
import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Serializes a stream of Z-set deltas to a Parquet file with bitemporal
 * {@code start_time} and {@code end_time} columns marking when each row was
 * added and removed from the underlying table or view.
 *
 * <p>Each unit of positive weight in an incoming delta is recorded as a live
 * row with a {@code start_time} stamp from the configured clock. When a
 * matching row is later retracted (negative weight), the live entry is popped
 * (FIFO) and a final record is emitted to the Parquet file with both
 * {@code start_time} and {@code end_time} set. Rows that remain live when the
 * serializer is closed are flushed with {@code end_time} equal to the close
 * timestamp.
 *
 * <p>The serializer takes ownership of nothing — call {@link #close()} to
 * flush remaining live rows and finalize the Parquet file.
 */
public final class ParquetSerializer implements AutoCloseable {

    public static final String START_TIME_COLUMN = "start_time";
    public static final String END_TIME_COLUMN = "end_time";

    private final Schema dataSchema;
    private final ParquetWriter<Object[]> writer;
    private final LongSupplier clock;
    private final Map<RowKey, Deque<Long>> live = new HashMap<>();
    private final List<Field> dataFields;
    private boolean closed;

    private ParquetSerializer(Schema dataSchema,
                              ParquetWriter<Object[]> writer,
                              LongSupplier clock) {
        this.dataSchema = dataSchema;
        this.writer = writer;
        this.clock = clock;
        this.dataFields = dataSchema.getFields();
    }

    /**
     * Create a serializer that writes to {@code outFile} using
     * {@link System#currentTimeMillis()} as the clock.
     */
    public static ParquetSerializer create(Schema dataSchema, File outFile) throws IOException {
        return create(dataSchema, outFile, System::currentTimeMillis);
    }

    /**
     * Create a serializer with a caller-supplied clock (useful for tests).
     */
    public static ParquetSerializer create(Schema dataSchema, File outFile, LongSupplier clock)
            throws IOException {
        MessageType messageType = buildMessageType(dataSchema);
        ParquetWriter<Object[]> writer = ParquetWriter.writeFile(
                messageType, outFile, recordDehydrator(dataSchema));
        return new ParquetSerializer(dataSchema, writer, clock);
    }

    /**
     * Process one delta: insertions (positive weights) start tracking live
     * rows; deletions (negative weights) match against live rows and emit
     * completed rows to the Parquet file.
     */
    public void recordDelta(ZSet delta) {
        if (closed) {
            throw new IllegalStateException("ParquetSerializer is closed");
        }
        if (delta == null) return;
        VectorSchemaRoot root = delta.data();
        int rowCount = root.getRowCount();
        if (rowCount == 0) return;

        int weightIdx = ArrowUtils.getWeightColumnIndex(root);
        IntVector weightVec = (IntVector) root.getVector(weightIdx);
        long now = clock.getAsLong();

        for (int row = 0; row < rowCount; row++) {
            int weight = weightVec.get(row);
            if (weight == 0) continue;
            Object[] values = readRow(root, row);
            RowKey key = new RowKey(values);
            if (weight > 0) {
                Deque<Long> q = live.computeIfAbsent(key, k -> new ArrayDeque<>());
                for (int i = 0; i < weight; i++) {
                    q.addLast(now);
                }
            } else {
                int retract = -weight;
                Deque<Long> q = live.get(key);
                for (int i = 0; i < retract; i++) {
                    Long startTime = (q != null) ? q.pollFirst() : null;
                    if (q != null && q.isEmpty()) {
                        live.remove(key);
                    }
                    writeRecord(values, startTime != null ? startTime : now, now);
                }
            }
        }
    }

    /**
     * Flush all remaining live rows and close the underlying Parquet writer.
     * Live rows are emitted with {@code end_time} set to the current clock.
     */
    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        long endTs = clock.getAsLong();
        try {
            for (Iterator<Map.Entry<RowKey, Deque<Long>>> it = live.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<RowKey, Deque<Long>> e = it.next();
                Object[] values = e.getKey().values;
                for (Long start : e.getValue()) {
                    writeRecord(values, start, endTs);
                }
                it.remove();
            }
        } finally {
            writer.close();
        }
    }

    // ---- internals ----

    private void writeRecord(Object[] values, long startTime, long endTime) {
        Object[] row = new Object[values.length + 2];
        System.arraycopy(values, 0, row, 0, values.length);
        row[values.length] = startTime;
        row[values.length + 1] = endTime;
        try {
            writer.write(row);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private Object[] readRow(VectorSchemaRoot root, int rowIndex) {
        Object[] values = new Object[dataFields.size()];
        for (int i = 0; i < values.length; i++) {
            FieldVector v = root.getVector(i);
            if (v.isNull(rowIndex)) {
                values[i] = null;
            } else {
                values[i] = ArrowUtils.getValue(v, rowIndex);
            }
        }
        return values;
    }

    private static MessageType buildMessageType(Schema dataSchema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field field : dataSchema.getFields()) {
            addField(builder, field);
        }
        builder.addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(START_TIME_COLUMN));
        builder.addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(END_TIME_COLUMN));
        return builder.named("invest_differential_view");
    }

    private static void addField(Types.MessageTypeBuilder builder, Field field) {
        ArrowType type = field.getType();
        boolean nullable = field.isNullable();
        Types.PrimitiveBuilder<PrimitiveType> pb;

        if (type instanceof ArrowType.Int intType) {
            int bits = intType.getBitWidth();
            if (bits <= 32) {
                pb = nullable
                        ? Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        : Types.required(PrimitiveType.PrimitiveTypeName.INT32);
            } else {
                pb = nullable
                        ? Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        : Types.required(PrimitiveType.PrimitiveTypeName.INT64);
            }
        } else if (type instanceof ArrowType.FloatingPoint fp) {
            if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                pb = nullable
                        ? Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                        : Types.required(PrimitiveType.PrimitiveTypeName.FLOAT);
            } else {
                pb = nullable
                        ? Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        : Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE);
            }
        } else if (type instanceof ArrowType.Bool) {
            pb = nullable
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    : Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN);
        } else if (type instanceof ArrowType.Utf8) {
            pb = (nullable
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    : Types.required(PrimitiveType.PrimitiveTypeName.BINARY))
                    .as(LogicalTypeAnnotation.stringType());
        } else if (type instanceof ArrowType.Date) {
            pb = nullable
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    : Types.required(PrimitiveType.PrimitiveTypeName.INT32);
        } else if (type instanceof ArrowType.Timestamp) {
            pb = nullable
                    ? Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    : Types.required(PrimitiveType.PrimitiveTypeName.INT64);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Arrow type for Parquet serialization: " + type);
        }
        builder.addField(pb.named(field.getName()));
    }

    private static Dehydrator<Object[]> recordDehydrator(Schema dataSchema) {
        List<Field> fields = dataSchema.getFields();
        return (record, valueWriter) -> {
            for (int i = 0; i < fields.size(); i++) {
                Object value = record[i];
                if (value == null) continue;
                Field f = fields.get(i);
                ArrowType type = f.getType();
                Object out = convertForParquet(type, value);
                valueWriter.write(f.getName(), out);
            }
            // start_time / end_time always present
            Object startTime = record[fields.size()];
            if (startTime != null) {
                valueWriter.write(START_TIME_COLUMN, ((Number) startTime).longValue());
            }
            Object endTime = record[fields.size() + 1];
            if (endTime != null) {
                valueWriter.write(END_TIME_COLUMN, ((Number) endTime).longValue());
            }
        };
    }

    private static Object convertForParquet(ArrowType type, Object value) {
        if (type instanceof ArrowType.Int intType) {
            long n = ((Number) value).longValue();
            if (intType.getBitWidth() <= 32) {
                return Integer.valueOf((int) n);
            }
            return Long.valueOf(n);
        } else if (type instanceof ArrowType.FloatingPoint fp) {
            double d = ((Number) value).doubleValue();
            if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                return Float.valueOf((float) d);
            }
            return Double.valueOf(d);
        } else if (type instanceof ArrowType.Bool) {
            return value instanceof Boolean b ? b : ((Number) value).intValue() != 0;
        } else if (type instanceof ArrowType.Utf8) {
            return value instanceof byte[] bs
                    ? new String(bs, StandardCharsets.UTF_8)
                    : value.toString();
        } else if (type instanceof ArrowType.Date) {
            return Integer.valueOf(((Number) value).intValue());
        } else if (type instanceof ArrowType.Timestamp) {
            return Long.valueOf(((Number) value).longValue());
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    /** Hashable wrapper around a row's data values (handles {@code byte[]}). */
    private static final class RowKey {
        private final Object[] values;
        private final int hash;

        RowKey(Object[] values) {
            this.values = values;
            int h = 1;
            for (Object v : values) {
                h = 31 * h + valueHash(v);
            }
            this.hash = h;
        }

        @Override
        public int hashCode() { return hash; }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof RowKey other)) return false;
            if (other.hash != hash || other.values.length != values.length) return false;
            for (int i = 0; i < values.length; i++) {
                if (!valueEquals(values[i], other.values[i])) return false;
            }
            return true;
        }

        private static int valueHash(Object v) {
            if (v == null) return 0;
            if (v instanceof byte[] b) return Arrays.hashCode(b);
            return v.hashCode();
        }

        private static boolean valueEquals(Object a, Object b) {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a instanceof byte[] ab && b instanceof byte[] bb) {
                return Arrays.equals(ab, bb);
            }
            return a.equals(b);
        }
    }
}
