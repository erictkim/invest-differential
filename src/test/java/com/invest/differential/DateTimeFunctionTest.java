package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DateTimeFunctionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = Arrays.asList(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }

    private static int epochDays(int year, int month, int day) {
        return (int) LocalDate.of(year, month, day).toEpochDay();
    }

    private static long epochMicros(int year, int month, int day, int hour, int minute, int second) {
        return LocalDateTime.of(year, month, day, hour, minute, second)
                .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
    }

    // --- Date column filtering and projection ---

    @Test
    void dateColumnFilterAndProject() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("event_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("events", schema)
                    .sql("SELECT id, event_date, amount FROM events WHERE amount > 100");

            engine.pushChanges("events", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, epochDays(2025, 1, 15), 200},
                    {2, epochDays(2025, 3, 20), 50},
                    {3, epochDays(2025, 6, 10), 300},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertTrue(rows.containsKey(List.of(1, epochDays(2025, 1, 15), 200)));
                assertTrue(rows.containsKey(List.of(3, epochDays(2025, 6, 10), 300)));
            }
        }
    }

    @Test
    void dateColumnIncrementalInsertAndDelete() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("hire_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("employees", schema)
                    .sql("SELECT name, hire_date FROM employees");

            // Step 1
            engine.pushChanges("employees", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", epochDays(2024, 1, 1)},
                    {"Bob", epochDays(2024, 6, 15)},
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(2, snap.rowCount());
            }

            // Step 2 — delete Alice, add Charlie
            ZSet combined;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", epochDays(2024, 1, 1)}
            })) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{
                             {"Charlie", epochDays(2025, 3, 1)}
                     })) {
                    combined = neg.add(ins);
                }
            }
            engine.pushChanges("employees", combined).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                Map<List<Object>, Integer> rows = toMap(snap);
                assertEquals(2, rows.size());
                assertTrue(rows.containsKey(List.of("Bob", epochDays(2024, 6, 15))));
                assertTrue(rows.containsKey(List.of("Charlie", epochDays(2025, 3, 1))));
            }
        }
    }

    @Test
    void dateGroupByAggregate() {
        Schema schema = new Schema(List.of(
                new Field("event_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                    .sql("SELECT event_date, SUM(amount) FROM sales GROUP BY event_date");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {epochDays(2025, 1, 1), 100},
                    {epochDays(2025, 1, 1), 200},
                    {epochDays(2025, 2, 1), 50},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(epochDays(2025, 1, 1), 300)));
                assertEquals(1, rows.get(List.of(epochDays(2025, 2, 1), 50)));
            }
        }
    }

    @Test
    void timestampColumnFilterAndProject() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("logs", schema)
                    .sql("SELECT id, ts, val FROM logs WHERE val > 10");

            long ts1 = epochMicros(2025, 1, 15, 10, 30, 0);
            long ts2 = epochMicros(2025, 3, 20, 14, 0, 0);
            long ts3 = epochMicros(2025, 6, 10, 9, 45, 0);

            engine.pushChanges("logs", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, ts1, 20},
                    {2, ts2, 5},
                    {3, ts3, 30},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertTrue(rows.containsKey(List.of(1, ts1, 20)));
                assertTrue(rows.containsKey(List.of(3, ts3, 30)));
            }
        }
    }

    // --- Extract function tests (evaluator-level) ---

    @Test
    void extractYearMonthDayFromDate() {
        var yearEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("YEAR"),
                        new com.invest.differential.expr.LiteralEvaluator(epochDays(2025, 7, 21))));
        assertEquals(2025L, yearEval.evaluate(null, 0));

        var monthEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("MONTH"),
                        new com.invest.differential.expr.LiteralEvaluator(epochDays(2025, 7, 21))));
        assertEquals(7L, monthEval.evaluate(null, 0));

        var dayEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("DAY"),
                        new com.invest.differential.expr.LiteralEvaluator(epochDays(2025, 7, 21))));
        assertEquals(21L, dayEval.evaluate(null, 0));
    }

    @Test
    void extractFromTimestamp() {
        long ts = epochMicros(2025, 3, 15, 14, 30, 45);

        var hourEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("HOUR"),
                        new com.invest.differential.expr.LiteralEvaluator(ts)));
        assertEquals(14L, hourEval.evaluate(null, 0));

        var minuteEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("MINUTE"),
                        new com.invest.differential.expr.LiteralEvaluator(ts)));
        assertEquals(30L, minuteEval.evaluate(null, 0));

        var secondEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("SECOND"),
                        new com.invest.differential.expr.LiteralEvaluator(ts)));
        assertEquals(45L, secondEval.evaluate(null, 0));
    }

    @Test
    void extractNull() {
        var eval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "extract",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("YEAR"),
                        new com.invest.differential.expr.LiteralEvaluator(null)));
        assertNull(eval.evaluate(null, 0));
    }

    // --- Date arithmetic (evaluator-level) ---

    @Test
    void addDateDays() {
        var eval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "add_date_days",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator(epochDays(2025, 1, 1)),
                        new com.invest.differential.expr.LiteralEvaluator(10)));
        Object result = eval.evaluate(null, 0);
        assertEquals(epochDays(2025, 1, 11), result);
    }

    @Test
    void subtractDateDays() {
        var eval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "subtract_date_days",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator(epochDays(2025, 3, 15)),
                        new com.invest.differential.expr.LiteralEvaluator(14)));
        Object result = eval.evaluate(null, 0);
        assertEquals(epochDays(2025, 3, 1), result);
    }

    @Test
    void dateArithmeticNull() {
        var eval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "add_date_days",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator(null),
                        new com.invest.differential.expr.LiteralEvaluator(10)));
        assertNull(eval.evaluate(null, 0));
    }

    // --- CastEvaluator date support ---

    @Test
    void castStringToDate() {
        var cast = new com.invest.differential.expr.CastEvaluator(
                new com.invest.differential.expr.LiteralEvaluator("2025-06-15"), "date");
        Object result = cast.evaluate(null, 0);
        assertEquals(epochDays(2025, 6, 15), result);
    }

    @Test
    void castDateNull() {
        var cast = new com.invest.differential.expr.CastEvaluator(
                new com.invest.differential.expr.LiteralEvaluator(null), "date");
        assertNull(cast.evaluate(null, 0));
    }

    // --- Date join ---

    @Test
    void dateColumnJoin() {
        Schema ordersSchema = new Schema(List.of(
                new Field("order_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        Schema promoSchema = new Schema(List.of(
                new Field("promo_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("discount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                    .registerTable("promos", promoSchema)
                    .sql("SELECT o.product, p.discount FROM orders o " +
                            "JOIN promos p ON o.order_date = p.promo_date");

            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {epochDays(2025, 1, 1), "Laptop"},
                    {epochDays(2025, 1, 2), "Mouse"},
                    {epochDays(2025, 1, 3), "Keyboard"},
            }));
            engine.pushChanges("promos", ZSet.fromData(promoSchema, allocator, new Object[][]{
                    {epochDays(2025, 1, 1), 10},
                    {epochDays(2025, 1, 3), 20},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Laptop", 10)));
                assertEquals(1, rows.get(List.of("Keyboard", 20)));
            }
        }
    }
}
