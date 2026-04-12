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

class CaseExpressionTest {

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
    void simpleCaseInSelect() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("students", schema)
                    .sql("SELECT name, CASE " +
                            "WHEN score >= 90 THEN 'A' " +
                            "WHEN score >= 80 THEN 'B' " +
                            "WHEN score >= 70 THEN 'C' " +
                            "ELSE 'F' END AS grade FROM students");

            engine.pushChanges("students", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 95}, {"Bob", 82}, {"Charlie", 73}, {"Dave", 55}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(4, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "A")));
                assertEquals(1, rows.get(List.of("Bob", "B")));
                assertEquals(1, rows.get(List.of("Charlie", "C")));
                assertEquals(1, rows.get(List.of("Dave", "F")));
            }
        }
    }

    @Test
    void caseWithoutElse() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT name, CASE WHEN val > 100 THEN 'high' END FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 200}, {"B", 50}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("A", "high")));
                // B's CASE result is null
                assertEquals(1, rows.get(Arrays.asList("B", null)));
            }
        }
    }

    @Test
    void caseInWhereClause() {
        Schema schema = new Schema(List.of(
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT category, amount FROM t WHERE " +
                            "CASE WHEN category = 'premium' THEN amount > 50 " +
                            "     ELSE amount > 200 END");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"premium", 60},    // passes (>50 for premium)
                    {"premium", 30},    // fails
                    {"standard", 300},  // passes (>200 for standard)
                    {"standard", 100},  // fails
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("premium", 60)));
                assertEquals(1, rows.get(List.of("standard", 300)));
            }
        }
    }

    @Test
    void caseWithArithmeticExpressions() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("qty", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT product, " +
                            "CASE WHEN qty >= 10 THEN price * qty * 9 / 10 " +
                            "     ELSE price * qty END AS total FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Widget", 100, 15},   // bulk: 100*15*9/10 = 1350
                    {"Gadget", 200, 3},    // normal: 200*3 = 600
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Widget", 1350)));
                assertEquals(1, rows.get(List.of("Gadget", 600)));
            }
        }
    }

    @Test
    void caseWithIncrementalUpdates() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT name, CASE WHEN score >= 50 THEN 'pass' ELSE 'fail' END FROM t");

            // Step 1
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 80}, {"Bob", 30}
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.get(List.of("Alice", "pass")));
                assertEquals(1, rows.get(List.of("Bob", "fail")));
            }

            // Step 2 — delete Bob=30, add Bob=60 (Bob now passes)
            ZSet combined;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{"Bob", 30}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{"Bob", 60}})) {
                    combined = neg.add(ins);
                }
            }
            engine.pushChanges("t", combined).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                assertEquals(-1, rows.get(List.of("Bob", "fail")));
                assertEquals(1, rows.get(List.of("Bob", "pass")));
            }
        }
    }

    @Test
    void nestedCaseExpressions() {
        Schema schema = new Schema(List.of(
                new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT x, CASE " +
                            "WHEN x > 0 THEN CASE WHEN x > 100 THEN 'big' ELSE 'small' END " +
                            "ELSE 'negative' END FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {200}, {50}, {-10}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(200, "big")));
                assertEquals(1, rows.get(List.of(50, "small")));
                assertEquals(1, rows.get(List.of(-10, "negative")));
            }
        }
    }

    @Test
    void caseWithGroupBy() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier, " +
                            "COUNT(*) FROM t GROUP BY CASE WHEN amount > 100 THEN 'high' ELSE 'low' END");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 50}, {"B", 200}, {"C", 150}, {"D", 30}, {"E", 300}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("high", 3L)));
                assertEquals(1, rows.get(List.of("low", 2L)));
            }
        }
    }

    @Test
    void caseWithNullHandling() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT name, CASE WHEN val IS NOT NULL THEN val * 2 ELSE 0 END FROM t");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 10}, {"B", null}, {"C", 25}
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("A", 20)));
                assertEquals(1, rows.get(List.of("B", 0)));
                assertEquals(1, rows.get(List.of("C", 50)));
            }
        }
    }
}
