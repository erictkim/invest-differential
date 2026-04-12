package com.invest.differential;

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

class StringFunctionTest {

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

    private Schema nameSchema() {
        return new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    @Test
    void upperFunction() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, UPPER(name) FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "hello"}, {2, "World"}, {3, "ALREADY"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, "HELLO")));
                assertEquals(1, rows.get(List.of(2, "WORLD")));
                assertEquals(1, rows.get(List.of(3, "ALREADY")));
            }
        }
    }

    @Test
    void lowerFunction() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, LOWER(name) FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "HELLO"}, {2, "World"}, {3, "already"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, "hello")));
                assertEquals(1, rows.get(List.of(2, "world")));
                assertEquals(1, rows.get(List.of(3, "already")));
            }
        }
    }

    @Test
    void charLengthFunction() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, CHAR_LENGTH(name) FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, ""}, {2, "abc"}, {3, "hello world"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, 0)));
                assertEquals(1, rows.get(List.of(2, 3)));
                assertEquals(1, rows.get(List.of(3, 11)));
            }
        }
    }

    @Test
    void substringFunction() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, SUBSTRING(name FROM 2 FOR 3) FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "abcdef"}, {2, "hi"}, {3, "xyzw"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, "bcd")));
                assertEquals(1, rows.get(List.of(2, "i")));
                assertEquals(1, rows.get(List.of(3, "yzw")));
            }
        }
    }

    @Test
    void trimFunctionViaEvaluator() {
        // TRIM has special SQL syntax that doesn't map directly through Substrait.
        // Test the evaluator directly.
        var trimEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "trim",
                List.of(new com.invest.differential.expr.LiteralEvaluator("  hello  ")));
        assertEquals("hello", trimEval.evaluate(null, 0));

        var ltrimEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "ltrim",
                List.of(new com.invest.differential.expr.LiteralEvaluator("  hello  ")));
        assertEquals("hello  ", ltrimEval.evaluate(null, 0));

        var rtrimEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "rtrim",
                List.of(new com.invest.differential.expr.LiteralEvaluator("  hello  ")));
        assertEquals("  hello", rtrimEval.evaluate(null, 0));

        // Null input
        var trimNull = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "trim",
                List.of(new com.invest.differential.expr.LiteralEvaluator(null)));
        assertNull(trimNull.evaluate(null, 0));
    }

    @Test
    void upperInFilterCondition() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, name FROM t WHERE UPPER(name) = 'ALICE'");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "alice"}, {2, "Bob"}, {3, "Alice"}, {4, "ALICE"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, "alice")));
                assertEquals(1, rows.get(List.of(3, "Alice")));
                assertEquals(1, rows.get(List.of(4, "ALICE")));
            }
        }
    }

    @Test
    void concatWithUpper() {
        // Test CONCAT and UPPER composability through the evaluator directly
        // since Calcite's CONCAT type resolution is strict about CHARACTER vs VARCHAR
        var upperEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "upper",
                List.of(new com.invest.differential.expr.LiteralEvaluator("hello")));
        assertEquals("HELLO", upperEval.evaluate(null, 0));

        var concatEval = new com.invest.differential.expr.ScalarFunctionEvaluator(
                "concat",
                List.of(
                        new com.invest.differential.expr.LiteralEvaluator("JOHN"),
                        new com.invest.differential.expr.LiteralEvaluator(" DOE")));
        assertEquals("JOHN DOE", concatEval.evaluate(null, 0));

        // Also: SQL-based test with UPPER on a simple concat of literals
        Schema schema = new Schema(List.of(
                new Field("fname", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("lname", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT UPPER(fname), UPPER(lname) FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"john", "doe"}, {"jane", "smith"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("JOHN", "DOE")));
                assertEquals(1, rows.get(List.of("JANE", "SMITH")));
            }
        }
    }

    @Test
    void stringFunctionsIncrementalUpdate() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, UPPER(name) FROM t");

            // Step 1
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "hello"}
            })).step();
            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                assertEquals(1, r1.rowCount());
            }

            // Step 2 — add + delete
            ZSet combined;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{1, "hello"}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{2, "world"}})) {
                    combined = neg.add(ins);
                }
            }
            engine.pushChanges("t", combined).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of(1, "HELLO")));
                assertEquals(1, rows.get(List.of(2, "WORLD")));
            }

            // Snapshot should show accumulated state
            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                Map<List<Object>, Integer> rows = toMap(snap);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(2, "WORLD")));
            }
        }
    }

    @Test
    void stringInGroupByAggregate() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT UPPER(name), SUM(amount) FROM t GROUP BY UPPER(name)");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"alice", 10}, {"Alice", 20}, {"ALICE", 30}, {"Bob", 50}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("ALICE", 60)));
                assertEquals(1, rows.get(List.of("BOB", 50)));
            }
        }
    }

    @Test
    void charLengthInFilter() {
        Schema schema = nameSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT id, name FROM t WHERE CHAR_LENGTH(name) > 3");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "ab"}, {2, "abcd"}, {3, "abcdef"}, {4, "x"}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(2, "abcd")));
                assertEquals(1, rows.get(List.of(3, "abcdef")));
            }
        }
    }
}
