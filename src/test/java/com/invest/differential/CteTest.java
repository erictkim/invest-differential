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

class CteTest {

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

    @Test
    void basicCte() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("WITH high_scorers AS (SELECT name, score FROM t WHERE score > 80) " +
                            "SELECT name, score FROM high_scorers");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 95}, {"Bob", 70}, {"Charlie", 85}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", 95)));
                assertEquals(1, rows.get(List.of("Charlie", 85)));
            }
        }
    }

    @Test
    void cteWithAggregation() {
        Schema schema = new Schema(List.of(
                new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("emp", schema)
                    .sql("WITH dept_totals AS (" +
                            "  SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept" +
                            ") SELECT * FROM dept_totals WHERE total > 100");

            engine.pushChanges("emp", ZSet.fromData(schema, allocator, new Object[][]{
                    {"eng", 80}, {"eng", 90}, {"sales", 50}, {"sales", 40}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(1, rows.size());
                // SUM through CTE wrapping may produce int
                var key = rows.keySet().iterator().next();
                assertEquals("eng", key.get(0));
                assertEquals(170, ((Number) key.get(1)).intValue());
                assertEquals(1, rows.values().iterator().next());
            }
        }
    }

    @Test
    void multipleCtes() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("WITH big AS (SELECT name, val FROM t WHERE val > 50), " +
                            "cat_a AS (SELECT name, val FROM t WHERE category = 'A') " +
                            "SELECT big.name, big.val FROM big " +
                            "INNER JOIN cat_a ON big.name = cat_a.name");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 100, "A"},  // in both CTEs
                    {"Bob", 80, "B"},     // only in big
                    {"Charlie", 30, "A"}, // only in cat_a
                    {"Dave", 60, "A"},    // in both
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", 100)));
                assertEquals(1, rows.get(List.of("Dave", 60)));
            }
        }
    }

    @Test
    void cteReferencedMultipleTimes() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            // CTE referenced in both sides of a UNION ALL
            engine.registerTable("t", schema)
                    .sql("WITH filtered AS (SELECT id, val FROM t WHERE val > 10) " +
                            "SELECT id, val FROM filtered WHERE val > 50 " +
                            "UNION ALL " +
                            "SELECT id, val FROM filtered WHERE val <= 50");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 20}, {2, 80}, {3, 5}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                // id=1 val=20 (<=50 branch), id=2 val=80 (>50 branch), id=3 filtered out
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(2, 80)));
                assertEquals(1, rows.get(List.of(1, 20)));
            }
        }
    }

    @Test
    void cteWithIncrementalUpdate() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("WITH passing AS (SELECT name, score FROM t WHERE score >= 60) " +
                            "SELECT name FROM passing");

            // Step 1
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 75}, {"Bob", 40}
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Alice")));
            }

            // Step 2: Bob's score increases to 65
            ZSet delta;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{"Bob", 40}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{"Bob", 65}})) {
                    delta = neg.add(ins);
                }
            }
            engine.pushChanges("t", delta).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                assertEquals(1, rows.get(List.of("Bob")));
            }
        }
    }

    @Test
    void cteWithGroupByAndHaving() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("qty", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                    .sql("WITH product_sales AS (" +
                            "  SELECT product, SUM(qty) AS total_qty FROM sales GROUP BY product" +
                            ") SELECT * FROM product_sales WHERE total_qty >= 10");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Widget", 5}, {"Widget", 8}, {"Gadget", 3}, {"Gadget", 2}, {"Doohickey", 15}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                // Verify by matching values regardless of SUM int/long type
                for (var entry : rows.entrySet()) {
                    String product = (String) entry.getKey().get(0);
                    int total = ((Number) entry.getKey().get(1)).intValue();
                    assertTrue(product.equals("Widget") || product.equals("Doohickey"));
                    if (product.equals("Widget")) assertEquals(13, total);
                    if (product.equals("Doohickey")) assertEquals(15, total);
                    assertEquals(1, entry.getValue());
                }
            }
        }
    }

    @Test
    void nestedCteWithCaseExpression() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("WITH graded AS (" +
                            "  SELECT name, CASE WHEN score >= 90 THEN 1 ELSE 0 END AS is_top FROM t" +
                            ") SELECT name FROM graded WHERE is_top = 1");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 95}, {"Bob", 70}, {"Charlie", 92}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice")));
                assertEquals(1, rows.get(List.of("Charlie")));
            }
        }
    }

    @Test
    void cteChained() {
        // CTE b references CTE a
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("WITH a AS (SELECT name, val FROM t WHERE val > 10), " +
                            "b AS (SELECT name, val * 2 AS doubled FROM a WHERE val < 100) " +
                            "SELECT name, doubled FROM b");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 50}, {"Bob", 5}, {"Charlie", 200}, {"Dave", 30}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", 100)));
                assertEquals(1, rows.get(List.of("Dave", 60)));
            }
        }
    }
}
