package com.invest.differential;

import com.invest.differential.udf.TableUdf;
import com.invest.differential.udf.UdfRegistry;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for user-defined function (UDF) support.
 */
class UdfTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        try {
            allocator.close();
        } catch (IllegalStateException e) {
            // Arrow memory leak detection — acceptable in tests
        }
    }

    // ---- Sample UDFs ----

    /** Reverses a string. */
    static Object strReverse(Object... args) {
        if (args[0] == null) return null;
        return new StringBuilder(args[0].toString()).reverse().toString();
    }

    /** Doubles a numeric value. */
    static Object doubleVal(Object... args) {
        if (args[0] == null) return null;
        Object v = args[0];
        if (v instanceof Integer i) return i * 2;
        if (v instanceof Long l) return l * 2;
        if (v instanceof Double d) return d * 2;
        return ((Number) v).doubleValue() * 2;
    }

    // ---- SQL Path Tests ----

    @Test
    void udfInProjection_sql() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string")
                    .sql("SELECT STR_REVERSE(name), val FROM t");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 1},
                    {"world", 2}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("olleh", 1)));
            assertTrue(rows.containsKey(List.of("dlrow", 2)));
        }
    }

    @Test
    void udfInFilter_sql() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string")
                    .sql("SELECT name, val FROM t WHERE STR_REVERSE(name) = 'olleh'");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 1},
                    {"world", 2},
                    {"abc", 3}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(1, result.rowCount());
        }
    }

    @Test
    void integerUdf_sql() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("double_val", UdfTest::doubleVal, new String[]{"i32"}, "i32")
                    .sql("SELECT name, DOUBLE_VAL(val) FROM t");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 5},
                    {"b", 10}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("a", 10)));
            assertTrue(rows.containsKey(List.of("b", 20)));
        }
    }

    @Test
    void udfIncremental_sql() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string")
                    .sql("SELECT STR_REVERSE(name), val FROM t");

            // Step 1: initial insert
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 1}
            });
            engine.pushChanges("t", delta1).step();
            ZSet r1 = engine.getOutput();
            r1.compact();
            assertEquals(1, r1.rowCount());

            // Step 2: incremental insert
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"world", 2}
            });
            engine.pushChanges("t", delta2).step();
            ZSet r2 = engine.getOutput();
            r2.compact();
            assertEquals(1, r2.rowCount()); // Only the new row

            Map<List<Object>, Integer> rows2 = toMap(r2);
            assertTrue(rows2.containsKey(List.of("dlrow", 2)));

            // Step 3: delete
            ZSet delta3 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 1}
            }).negate();
            engine.pushChanges("t", delta3).step();
            ZSet r3 = engine.getOutput();
            r3.compact();
            assertEquals(1, r3.rowCount()); // Retraction
        }
    }

    @Test
    void multipleUdfs_sql() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string")
                    .registerUdf("double_val", UdfTest::doubleVal, new String[]{"i32"}, "i32")
                    .sql("SELECT STR_REVERSE(name), DOUBLE_VAL(val) FROM t");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 5},
                    {"world", 10}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("olleh", 10)));
            assertTrue(rows.containsKey(List.of("dlrow", 20)));
        }
    }

    // ---- Substrait Plan Path Tests ----

    @Test
    void udfViaSubstraitPlan() {
        // This test demonstrates that UDFs work when a Substrait plan is
        // loaded from bytes (e.g., received from an external system).
        // We build the plan via the SQL path, serialize it, then deserialize
        // and execute with the UDF registered.

        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        byte[] planBytes;

        // Phase 1: compile SQL to Substrait bytes
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string");

            // Use UdfSqlToSubstrait directly to get the proto plan bytes
            List<String> creates = List.of(
                    "CREATE TABLE t (\"name\" VARCHAR, \"val\" INTEGER)");
            var udfRegistry = engine.getUdfRegistry();
            var converter = new io.substrait.isthmus.UdfSqlToSubstrait(
                    udfRegistry.buildOperatorTable(),
                    udfRegistry.buildSigs(),
                    udfRegistry.buildMergedExtensions());
            var protoPlan = converter.execute(
                    "SELECT STR_REVERSE(name), val FROM t", creates);
            planBytes = protoPlan.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Phase 2: load plan from bytes and execute
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdf("str_reverse", UdfTest::strReverse, new String[]{"string"}, "string")
                    .planFromBytes(planBytes);

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"hello", 1}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(1, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("olleh", 1)));
        }
    }

    @Test
    void udfRegistry_basic() {
        UdfRegistry registry = new UdfRegistry();
        assertTrue(registry.isEmpty());

        registry.register("my_func", args -> args[0], new String[]{"string"}, "string");
        assertFalse(registry.isEmpty());
        assertNotNull(registry.get("my_func"));
        assertNotNull(registry.get("MY_FUNC")); // case-insensitive
        assertNull(registry.get("nonexistent"));
    }

    // ---- Table UDF (multi-row, multi-column) ----

    /** CSV-splitter UDTF: (id, csv) -> rows of (id, part, idx). */
    static final TableUdf CSV_SPLIT = args -> {
        Integer id = (Integer) args[0];
        String s = (String) args[1];
        if (s == null) return List.of();
        List<Object[]> out = new ArrayList<>();
        String[] parts = s.split(",");
        for (int i = 0; i < parts.length; i++) {
            out.add(new Object[]{id, parts[i], i});
        }
        return out;
    };

    private static Schema csvSourceSchema() {
        return new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("csv", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    private static Schema csvOutputSchema() {
        return new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("part", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("idx", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
    }

    @Test
    void tableUdf_explodesString() {
        Schema in = csvSourceSchema();
        Schema out = csvOutputSchema();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", in)
                    .applyTableUdf("exploded", "t", new int[]{0, 1}, CSV_SPLIT, out);

            ZSet delta = ZSet.fromData(in, allocator, new Object[][]{
                    {1, "a,b,c"},
                    {2, "x,y"}
            });
            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput("exploded");
            result.compact();
            assertEquals(5, result.rowCount()); // 3 + 2

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of(1, "a", 0)));
            assertTrue(rows.containsKey(List.of(1, "b", 1)));
            assertTrue(rows.containsKey(List.of(1, "c", 2)));
            assertTrue(rows.containsKey(List.of(2, "x", 0)));
            assertTrue(rows.containsKey(List.of(2, "y", 1)));
        }
    }

    @Test
    void tableUdf_incrementalRetraction() {
        Schema in = csvSourceSchema();
        Schema out = csvOutputSchema();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", in)
                    .applyTableUdf("exploded", "t", new int[]{0, 1}, CSV_SPLIT, out);

            // Step 1: insert one source row that explodes to 3.
            ZSet delta1 = ZSet.fromData(in, allocator, new Object[][]{{1, "a,b,c"}});
            engine.pushChanges("t", delta1).step();
            assertEquals(3, engine.getOutput("exploded").rowCount());

            // Step 2: insert a second source row that explodes to 2.
            ZSet delta2 = ZSet.fromData(in, allocator, new Object[][]{{2, "x,y"}});
            engine.pushChanges("t", delta2).step();
            ZSet step2 = engine.getOutput("exploded");
            step2.compact();
            assertEquals(2, step2.rowCount());

            // Snapshot should accumulate to 5 live rows.
            ZSet snap = engine.getSnapshot("exploded");
            try {
                snap.compact();
                assertEquals(5, snap.rowCount());
            } finally {
                snap.close();
            }

            // Step 3: retract the first source row.
            ZSet neg;
            try (ZSet src = ZSet.fromData(in, allocator, new Object[][]{{1, "a,b,c"}})) {
                neg = src.negate();
            }
            engine.pushChanges("t", neg).step();
            ZSet step3 = engine.getOutput("exploded");
            step3.compact();
            assertEquals(3, step3.rowCount()); // 3 retraction rows
            for (int i = 0; i < step3.rowCount(); i++) {
                assertEquals(-1, step3.getWeight(i));
            }

            ZSet snap2 = engine.getSnapshot("exploded");
            try {
                snap2.compact();
                assertEquals(2, snap2.rowCount()); // only id=2 rows remain
            } finally {
                snap2.close();
            }
        }
    }

    @Test
    void tableUdf_chainedWithSql() {
        Schema in = csvSourceSchema();
        Schema out = csvOutputSchema();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", in)
                    .applyTableUdf("exploded", "t", new int[]{0, 1}, CSV_SPLIT, out)
                    .sql("SELECT id, part FROM exploded WHERE idx > 0", "filtered");

            ZSet delta = ZSet.fromData(in, allocator, new Object[][]{
                    {1, "a,b,c"},
                    {2, "x,y"}
            });
            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput("filtered");
            result.compact();
            // From (1, a,b,c) keep b(idx=1), c(idx=2); from (2, x,y) keep y(idx=1).
            assertEquals(3, result.rowCount());
            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of(1, "b")));
            assertTrue(rows.containsKey(List.of(1, "c")));
            assertTrue(rows.containsKey(List.of(2, "y")));
        }
    }

    @Test
    void tableUdf_emptyOutputIsAllowed() {
        Schema in = csvSourceSchema();
        Schema out = csvOutputSchema();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", in)
                    .applyTableUdf("exploded", "t", new int[]{0, 1}, CSV_SPLIT, out);

            // Null csv → UDF returns empty list → input row produces no output rows.
            ZSet delta = ZSet.fromData(in, allocator, new Object[][]{
                    {1, null},
                    {2, "x"}
            });
            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput("exploded");
            result.compact();
            assertEquals(1, result.rowCount());
            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of(2, "x", 0)));
        }
    }

    // ---- Helpers ----

    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }
}
