package com.invest.differential;

import com.invest.differential.expr.ExpressionEvaluator;
import com.invest.differential.expr.LiteralEvaluator;
import com.invest.differential.expr.ScalarFunctionEvaluator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AdditionalDateTimeFunctionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    // ── Helper: create a single-date-column batch ──

    private VectorSchemaRoot createDateBatch(LocalDate... dates) {
        Schema schema = new Schema(List.of(
                new Field("d", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        DateDayVector v = (DateDayVector) root.getVector(0);
        for (int i = 0; i < dates.length; i++) {
            v.set(i, (int) dates[i].toEpochDay());
        }
        root.setRowCount(dates.length);
        return root;
    }

    private VectorSchemaRoot createTimestampBatch(LocalDateTime... datetimes) {
        Schema schema = new Schema(List.of(
                new Field("ts", FieldType.nullable(
                        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        TimeStampMicroVector v = (TimeStampMicroVector) root.getVector(0);
        for (int i = 0; i < datetimes.length; i++) {
            long micros = datetimes[i].toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
            v.set(i, micros);
        }
        root.setRowCount(datetimes.length);
        return root;
    }

    private ExpressionEvaluator fieldRef(int index) {
        return new com.invest.differential.expr.FieldReferenceEvaluator(index);
    }

    private ExpressionEvaluator lit(Object value) {
        return new LiteralEvaluator(value);
    }

    // ── DATE_TRUNC on dates ──

    @Test
    void dateTruncYearOnDate() {
        try (VectorSchemaRoot batch = createDateBatch(
                LocalDate.of(2024, 3, 15),
                LocalDate.of(2024, 7, 4),
                LocalDate.of(2025, 12, 31))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("YEAR"), fieldRef(0)));
            for (int i = 0; i < 3; i++) {
                int result = (int) eval.evaluate(batch, i);
                LocalDate truncated = LocalDate.ofEpochDay(result);
                assertEquals(1, truncated.getMonthValue());
                assertEquals(1, truncated.getDayOfMonth());
            }
            // First two should be Jan 1, 2024
            assertEquals(LocalDate.of(2024, 1, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 0)));
            // Third should be Jan 1, 2025
            assertEquals(LocalDate.of(2025, 1, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 2)));
        }
    }

    @Test
    void dateTruncMonthOnDate() {
        try (VectorSchemaRoot batch = createDateBatch(
                LocalDate.of(2024, 3, 15),
                LocalDate.of(2024, 3, 28),
                LocalDate.of(2024, 6, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("MONTH"), fieldRef(0)));
            assertEquals(LocalDate.of(2024, 3, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 0)));
            assertEquals(LocalDate.of(2024, 3, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 1)));
            assertEquals(LocalDate.of(2024, 6, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 2)));
        }
    }

    @Test
    void dateTruncQuarterOnDate() {
        try (VectorSchemaRoot batch = createDateBatch(
                LocalDate.of(2024, 2, 10),   // Q1
                LocalDate.of(2024, 5, 20),   // Q2
                LocalDate.of(2024, 8, 15),   // Q3
                LocalDate.of(2024, 11, 1)))  // Q4
        {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("QUARTER"), fieldRef(0)));
            assertEquals(LocalDate.of(2024, 1, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 0)));
            assertEquals(LocalDate.of(2024, 4, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 1)));
            assertEquals(LocalDate.of(2024, 7, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 2)));
            assertEquals(LocalDate.of(2024, 10, 1),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 3)));
        }
    }

    @Test
    void dateTruncWeekOnDate() {
        // 2024-03-14 is Thursday → Monday = 2024-03-11
        try (VectorSchemaRoot batch = createDateBatch(
                LocalDate.of(2024, 3, 14))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("WEEK"), fieldRef(0)));
            assertEquals(LocalDate.of(2024, 3, 11),
                    LocalDate.ofEpochDay((int) eval.evaluate(batch, 0)));
        }
    }

    // ── DATE_TRUNC on timestamps ──

    @Test
    void dateTruncHourOnTimestamp() {
        try (VectorSchemaRoot batch = createTimestampBatch(
                LocalDateTime.of(2024, 3, 15, 14, 35, 22))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("HOUR"), fieldRef(0)));
            long resultMicros = (long) eval.evaluate(batch, 0);
            LocalDateTime result = Instant.ofEpochMilli(resultMicros / 1000)
                    .atZone(ZoneOffset.UTC).toLocalDateTime();
            assertEquals(LocalDateTime.of(2024, 3, 15, 14, 0, 0), result);
        }
    }

    @Test
    void dateTruncDayOnTimestamp() {
        try (VectorSchemaRoot batch = createTimestampBatch(
                LocalDateTime.of(2024, 3, 15, 14, 35, 22))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("DAY"), fieldRef(0)));
            long resultMicros = (long) eval.evaluate(batch, 0);
            LocalDateTime result = Instant.ofEpochMilli(resultMicros / 1000)
                    .atZone(ZoneOffset.UTC).toLocalDateTime();
            assertEquals(LocalDateTime.of(2024, 3, 15, 0, 0, 0), result);
        }
    }

    // ── DATE_DIFF on dates ──

    @Test
    void dateDiffDaysBetweenDates() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 31),
                LocalDate.of(2024, 3, 1), LocalDate.of(2024, 3, 15))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("DAY"), fieldRef(0), fieldRef(1)));
            assertEquals(30L, eval.evaluate(batch, 0));
            assertEquals(14L, eval.evaluate(batch, 1));
        }
    }

    @Test
    void dateDiffMonthsBetweenDates() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2024, 1, 15), LocalDate.of(2024, 4, 15),
                LocalDate.of(2024, 1, 1), LocalDate.of(2025, 1, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("MONTH"), fieldRef(0), fieldRef(1)));
            assertEquals(3L, eval.evaluate(batch, 0));
            assertEquals(12L, eval.evaluate(batch, 1));
        }
    }

    @Test
    void dateDiffYearsBetweenDates() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2020, 6, 1), LocalDate.of(2024, 6, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("YEAR"), fieldRef(0), fieldRef(1)));
            assertEquals(4L, eval.evaluate(batch, 0));
        }
    }

    @Test
    void dateDiffWeeksBetweenDates() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 22))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("WEEK"), fieldRef(0), fieldRef(1)));
            assertEquals(3L, eval.evaluate(batch, 0));
        }
    }

    // ── DATE_DIFF on timestamps ──

    @Test
    void dateDiffHoursBetweenTimestamps() {
        try (VectorSchemaRoot batch = createTwoTimestampBatch(
                LocalDateTime.of(2024, 1, 1, 10, 0, 0),
                LocalDateTime.of(2024, 1, 1, 15, 30, 0))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("HOUR"), fieldRef(0), fieldRef(1)));
            assertEquals(5L, eval.evaluate(batch, 0));
        }
    }

    @Test
    void dateDiffSecondsBetweenTimestamps() {
        try (VectorSchemaRoot batch = createTwoTimestampBatch(
                LocalDateTime.of(2024, 1, 1, 10, 0, 0),
                LocalDateTime.of(2024, 1, 1, 10, 1, 30))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("SECOND"), fieldRef(0), fieldRef(1)));
            assertEquals(90L, eval.evaluate(batch, 0));
        }
    }

    // ── CURRENT_DATE and CURRENT_TIMESTAMP ──

    @Test
    void currentDateReturnsEpochDays() {
        try (VectorSchemaRoot batch = createDateBatch(LocalDate.of(2024, 1, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("current_date", List.of());
            Object result = eval.evaluate(batch, 0);
            assertNotNull(result);
            int epochDays = (int) result;
            LocalDate today = LocalDate.ofEpochDay(epochDays);
            assertEquals(LocalDate.now(), today);
        }
    }

    @Test
    void currentTimestampReturnsMicros() {
        try (VectorSchemaRoot batch = createDateBatch(LocalDate.of(2024, 1, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("current_timestamp", List.of());
            Object result = eval.evaluate(batch, 0);
            assertNotNull(result);
            long micros = (long) result;
            // Should be within 5 seconds of now
            long nowMicros = Instant.now().toEpochMilli() * 1000L;
            assertTrue(Math.abs(nowMicros - micros) < 5_000_000L,
                    "current_timestamp should be close to now");
        }
    }

    // ── Null handling ──

    @Test
    void dateTruncNullReturnsNull() {
        Schema schema = new Schema(List.of(
                new Field("d", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null)
        ));
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            ((DateDayVector) batch.getVector(0)).setNull(0);
            batch.setRowCount(1);

            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_trunc",
                    List.of(lit("YEAR"), fieldRef(0)));
            assertNull(eval.evaluate(batch, 0));
        }
    }

    @Test
    void dateDiffNullReturnsNull() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 31))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("DAY"), lit(null), fieldRef(1)));
            assertNull(eval.evaluate(batch, 0));
        }
    }

    // ── Negative date_diff (date2 < date1) ──

    @Test
    void dateDiffNegativeWhenReversed() {
        try (VectorSchemaRoot batch = createTwoDateBatch(
                LocalDate.of(2024, 3, 15), LocalDate.of(2024, 3, 1))) {
            ExpressionEvaluator eval = new ScalarFunctionEvaluator("date_diff",
                    List.of(lit("DAY"), fieldRef(0), fieldRef(1)));
            assertEquals(-14L, eval.evaluate(batch, 0));
        }
    }

    // ── Helpers for two-column date/timestamp batches ──

    private VectorSchemaRoot createTwoDateBatch(LocalDate... alternating) {
        int rows = alternating.length / 2;
        Schema schema = new Schema(List.of(
                new Field("d1", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null),
                new Field("d2", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        DateDayVector v1 = (DateDayVector) root.getVector(0);
        DateDayVector v2 = (DateDayVector) root.getVector(1);
        for (int i = 0; i < rows; i++) {
            v1.set(i, (int) alternating[i * 2].toEpochDay());
            v2.set(i, (int) alternating[i * 2 + 1].toEpochDay());
        }
        root.setRowCount(rows);
        return root;
    }

    private VectorSchemaRoot createTwoTimestampBatch(LocalDateTime... alternating) {
        int rows = alternating.length / 2;
        Schema schema = new Schema(List.of(
                new Field("ts1", FieldType.nullable(
                        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)), null),
                new Field("ts2", FieldType.nullable(
                        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        TimeStampMicroVector v1 = (TimeStampMicroVector) root.getVector(0);
        TimeStampMicroVector v2 = (TimeStampMicroVector) root.getVector(1);
        for (int i = 0; i < rows; i++) {
            v1.set(i, alternating[i * 2].toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
            v2.set(i, alternating[i * 2 + 1].toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
        }
        root.setRowCount(rows);
        return root;
    }
}
