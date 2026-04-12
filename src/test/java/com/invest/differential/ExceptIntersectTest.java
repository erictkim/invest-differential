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

class ExceptIntersectTest {

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
    void basicExcept() {
        Schema schemaA = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        Schema schemaB = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schemaA)
                    .registerTable("b", schemaB)
                    .sql("SELECT id FROM a EXCEPT SELECT id FROM b");

            engine.pushChanges("a", ZSet.fromData(schemaA, allocator, new Object[][]{
                    {1}, {2}, {3}, {4}
            }));
            engine.pushChanges("b", ZSet.fromData(schemaB, allocator, new Object[][]{
                    {2}, {4}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(1)));
                assertEquals(1, rows.get(List.of(3)));
            }
        }
    }

    @Test
    void basicIntersect() {
        Schema schemaA = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        Schema schemaB = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schemaA)
                    .registerTable("b", schemaB)
                    .sql("SELECT id FROM a INTERSECT SELECT id FROM b");

            engine.pushChanges("a", ZSet.fromData(schemaA, allocator, new Object[][]{
                    {1}, {2}, {3}, {4}
            }));
            engine.pushChanges("b", ZSet.fromData(schemaB, allocator, new Object[][]{
                    {2}, {4}, {5}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(2)));
                assertEquals(1, rows.get(List.of(4)));
            }
        }
    }

    @Test
    void exceptWithMultipleColumns() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT name, val FROM a EXCEPT SELECT name, val FROM b");

            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 10}, {"Bob", 20}, {"Charlie", 30}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Bob", 20}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", 10)));
                assertEquals(1, rows.get(List.of("Charlie", 30)));
            }
        }
    }

    @Test
    void intersectWithMultipleColumns() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT name, val FROM a INTERSECT SELECT name, val FROM b");

            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 10}, {"Bob", 20}, {"Charlie", 30}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Bob", 20}, {"Charlie", 30}, {"Dave", 40}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Bob", 20)));
                assertEquals(1, rows.get(List.of("Charlie", 30)));
            }
        }
    }

    @Test
    void exceptIncrementalAddToRight() {
        // Adding a row to right side should remove it from the except output
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a EXCEPT SELECT id FROM b");

            // Step 1: a={1,2,3}, b={2}. Result: {1,3}
            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1}, {2}, {3}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {2}
            }));
            engine.step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(1)));
                assertEquals(1, rows.get(List.of(3)));
            }

            // Step 2: Add 3 to b. Now a={1,2,3}, b={2,3}. Result should remove 3
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {3}
            }));
            engine.step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Delta: -3 from output
                assertEquals(-1, rows.get(List.of(3)));
            }
        }
    }

    @Test
    void intersectIncrementalAddToLeft() {
        // Adding a row to left that exists in right should produce new intersection row
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a INTERSECT SELECT id FROM b");

            // Step 1: a={1,2}, b={2,3}. Intersect: {2}
            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1}, {2}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {2}, {3}
            }));
            engine.step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(2)));
            }

            // Step 2: Add 3 to a. Now a={1,2,3}, b={2,3}. Intersect: {2,3}
            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {3}
            }));
            engine.step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Delta: +3
                assertEquals(1, rows.get(List.of(3)));
            }
        }
    }

    @Test
    void exceptDisjointSets() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a EXCEPT SELECT id FROM b");

            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1}, {2}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {3}, {4}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of(1)));
                assertEquals(1, rows.get(List.of(2)));
            }
        }
    }

    @Test
    void intersectEmptyResult() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a INTERSECT SELECT id FROM b");

            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1}, {2}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {3}, {4}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                assertEquals(0, result.rowCount());
            }
        }
    }

    @Test
    void exceptRemoveFromRight() {
        // Removing row from right side should add it back to except result
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a EXCEPT SELECT id FROM b");

            // Step 1: a={1,2,3}, b={2,3}. Result: {1}
            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1}, {2}, {3}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {2}, {3}
            }));
            engine.step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(1)));
            }

            // Step 2: Remove 2 from b. Now a={1,2,3}, b={3}. Result: {1,2}
            ZSet del;
            try (ZSet neg = ZSet.fromData(schema, allocator, new Object[][]{{2}})) {
                del = neg.negate();
            }
            engine.pushChanges("b", del);
            engine.step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Delta: +2 appeared in output
                assertEquals(1, rows.get(List.of(2)));
            }
        }
    }

    @Test
    void intersectWithProjection() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT name FROM a INTERSECT SELECT name FROM b");

            engine.pushChanges("a", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Alice", 10}, {"Bob", 20}, {"Charlie", 30}
            }));
            engine.pushChanges("b", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Bob", 99}, {"Dave", 40}
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Bob")));
            }
        }
    }
}
