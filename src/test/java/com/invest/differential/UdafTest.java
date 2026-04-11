package com.invest.differential;

import com.invest.differential.udf.AggregateUdf;
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
 * Tests for user-defined aggregate functions (UDAFs).
 */
class UdafTest {

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
            // Arrow memory leak detection
        }
    }

    // ---- Sample UDAFs ----

    /** Product aggregate: multiplies all values. */
    static final AggregateUdf PRODUCT = new AggregateUdf() {
        @Override public Object initialize() { return 1L; }
        @Override public Object accumulate(Object acc, Object value, int weight) {
            if (value == null) return acc;
            long current = ((Number) acc).longValue();
            long val = ((Number) value).longValue();
            // For weight-aware: val^weight (simplified to multiply for +1, divide for -1)
            for (int i = 0; i < Math.abs(weight); i++) {
                if (weight > 0) current *= val;
                else current /= val;
            }
            return current;
        }
        @Override public Object finalize(Object acc) { return acc; }
    };

    /** Weighted sum: sum(value * weight), weight-aware by design. */
    static final AggregateUdf WEIGHTED_SUM = new AggregateUdf() {
        @Override public Object initialize() { return 0L; }
        @Override public Object accumulate(Object acc, Object value, int weight) {
            if (value == null) return acc;
            long current = ((Number) acc).longValue();
            long val = ((Number) value).longValue();
            return current + val * weight;
        }
        @Override public Object finalize(Object acc) { return acc; }
    };

    /** String concatenation aggregate (concatenates in insertion order). */
    static final AggregateUdf STRING_AGG = new AggregateUdf() {
        @Override public Object initialize() { return ""; }
        @Override public Object accumulate(Object acc, Object value, int weight) {
            if (value == null || weight <= 0) return acc;
            String current = (String) acc;
            String val = value.toString();
            for (int i = 0; i < weight; i++) {
                if (!current.isEmpty()) current += ",";
                current += val;
            }
            return current;
        }
        @Override public Object finalize(Object acc) { return acc; }
    };

    // ---- Tests ----

    @Test
    void udafBasicGroupBy() {
        Schema schema = new Schema(List.of(
                new Field("grp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdaf("wsum", WEIGHTED_SUM, "i32", "i64")
                    .sql("SELECT grp, WSUM(val) FROM t GROUP BY grp");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 10},
                    {"a", 20},
                    {"b", 5}
            });

            engine.pushChanges("t", delta).step();
            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("a", 30L)));
            assertTrue(rows.containsKey(List.of("b", 5L)));
        }
    }

    @Test
    void udafIncremental() {
        Schema schema = new Schema(List.of(
                new Field("grp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdaf("wsum", WEIGHTED_SUM, "i32", "i64")
                    .sql("SELECT grp, WSUM(val) FROM t GROUP BY grp");

            // Step 1: initial data
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 10},
                    {"b", 20}
            });
            engine.pushChanges("t", delta1).step();
            ZSet r1 = engine.getOutput();
            r1.compact();
            assertEquals(2, r1.rowCount());

            // Step 2: insert more into group "a"
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 5}
            });
            engine.pushChanges("t", delta2).step();
            ZSet r2 = engine.getOutput();
            r2.compact();
            // Output is the diff: a changed from 10 to 15
            Map<List<Object>, Integer> rows2 = toMap(r2);
            assertTrue(rows2.containsKey(List.of("a", 15L))); // new value
            assertTrue(rows2.containsKey(List.of("a", 10L))); // retraction of old value

            // Step 3: retract from group "b"
            ZSet delta3 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"b", 20}
            }).negate();
            engine.pushChanges("t", delta3).step();
            ZSet r3 = engine.getOutput();
            r3.compact();
            Map<List<Object>, Integer> rows3 = toMap(r3);
            // b:20 retracted, group b becomes 0
            assertTrue(rows3.containsKey(List.of("b", 20L))); // retraction
        }
    }

    @Test
    void udafWithBuiltinAggregate() {
        // Use a UDAF alongside a built-in aggregate
        Schema schema = new Schema(List.of(
                new Field("grp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdaf("wsum", WEIGHTED_SUM, "i32", "i64")
                    .sql("SELECT grp, WSUM(val), COUNT(val) FROM t GROUP BY grp");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 10},
                    {"a", 20},
                    {"b", 5}
            });

            engine.pushChanges("t", delta).step();
            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            // a: wsum=30, count=2
            assertTrue(rows.containsKey(List.of("a", 30L, 2L)));
            // b: wsum=5, count=1
            assertTrue(rows.containsKey(List.of("b", 5L, 1L)));
        }
    }

    @Test
    void udafProduct() {
        Schema schema = new Schema(List.of(
                new Field("grp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .registerUdaf("my_product", PRODUCT, "i32", "i64")
                    .sql("SELECT grp, MY_PRODUCT(val) FROM t GROUP BY grp");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"a", 2},
                    {"a", 3},
                    {"b", 7}
            });

            engine.pushChanges("t", delta).step();
            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());

            Map<List<Object>, Integer> rows = toMap(result);
            assertTrue(rows.containsKey(List.of("a", 6L)));
            assertTrue(rows.containsKey(List.of("b", 7L)));
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
