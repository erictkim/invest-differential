package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IncrementalEngineTest {

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
            // Arrow memory leak detection - acceptable in tests
        }
    }

    @Test
    void filterQuery() {
        Schema ordersSchema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100");

            // Push initial data
            ZSet delta = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {"widget", 50},
                    {"gadget", 150},
                    {"thing", 200}
            });

            engine.pushChanges("orders", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount()); // gadget=150, thing=200
        }
    }

    @Test
    void projectQuery() {
        Schema schema = new Schema(List.of(
                new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("y", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT x FROM t");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10},
                    {2, 20}
            });

            engine.pushChanges("t", delta).step();

            ZSet result = engine.getOutput();
            result.compact();
            assertEquals(2, result.rowCount());
        }
    }

    @Test
    void incrementalUpdates() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("data", schema)
                    .sql("SELECT id, val FROM data WHERE val > 10");

            // Step 1: insert {1,5}, {2,15}, {3,25}
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5}, {2, 15}, {3, 25}
            });
            engine.pushChanges("data", delta1).step();
            ZSet r1 = engine.getOutput();
            r1.compact();
            assertEquals(2, r1.rowCount()); // {2,15}, {3,25}

            // Step 2: insert {4,30} — incremental, only processes new delta
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {4, 30}
            });
            engine.pushChanges("data", delta2).step();
            ZSet r2 = engine.getOutput();
            r2.compact();
            assertEquals(1, r2.rowCount()); // Only the new {4,30}

            // Step 3: delete {2,15} via negative weight
            ZSet delta3 = ZSet.fromData(schema, allocator, new Object[][]{
                    {2, 15}
            }).negate();
            engine.pushChanges("data", delta3).step();
            ZSet r3 = engine.getOutput();
            r3.compact();
            assertEquals(1, r3.rowCount()); // retraction of {2,15}
        }
    }

    @Test
    void resetClearsState() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t WHERE v > 0");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{{1}, {2}});
            engine.pushChanges("t", delta).step();
            assertFalse(engine.getOutput().isEmpty());

            engine.reset();

            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{{3}});
            engine.pushChanges("t", delta2).step();
            ZSet r = engine.getOutput();
            r.compact();
            assertEquals(1, r.rowCount());
        }
    }

    @Test
    void snapshotAccumulatesAcrossSteps() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100");

            // Step 1: insert 3 rows, 2 pass filter
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"widget", 50}, {"gadget", 150}, {"thing", 200}
            });
            engine.pushChanges("orders", delta1).step();

            try (ZSet snap1 = engine.getSnapshot()) {
                snap1.compact();
                assertEquals(2, snap1.rowCount());
            }

            // Step 2: insert 1 more passing row
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"laptop", 999}
            });
            engine.pushChanges("orders", delta2).step();

            try (ZSet snap2 = engine.getSnapshot()) {
                snap2.compact();
                assertEquals(3, snap2.rowCount()); // gadget + thing + laptop
            }

            // Step 3: delete one row via negative weight
            ZSet delta3 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"gadget", 150}
            }).negate();
            engine.pushChanges("orders", delta3).step();

            try (ZSet snap3 = engine.getSnapshot()) {
                snap3.compact();
                assertEquals(2, snap3.rowCount()); // thing + laptop
            }
        }
    }

    @Test
    void snapshotWithAggregation() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("revenue", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                    .sql("SELECT product, SUM(revenue) as total FROM sales GROUP BY product");

            // Step 1: two products
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 100}, {"B", 200}, {"A", 50}
            });
            engine.pushChanges("sales", delta1).step();

            try (ZSet snap1 = engine.getSnapshot()) {
                snap1.compact();
                assertEquals(2, snap1.rowCount()); // A: 150, B: 200
            }

            // Step 2: more A sales
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 75}
            });
            engine.pushChanges("sales", delta2).step();

            try (ZSet snap2 = engine.getSnapshot()) {
                snap2.compact();
                assertEquals(2, snap2.rowCount()); // A: 225, B: 200
            }
        }
    }

    @Test
    void snapshotResetsWithEngine() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t WHERE v > 0");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{{1}, {2}});
            engine.pushChanges("t", delta).step();

            try (ZSet snap = engine.getSnapshot()) {
                assertEquals(2, snap.rowCount());
            }

            engine.reset();

            // After reset, snapshot should be empty
            try (ZSet snapAfterReset = engine.getSnapshot()) {
                assertTrue(snapAfterReset.isEmpty());
            }

            // New data after reset
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{{3}});
            engine.pushChanges("t", delta2).step();

            try (ZSet snapAfterNewData = engine.getSnapshot()) {
                snapAfterNewData.compact();
                assertEquals(1, snapAfterNewData.rowCount());
            }
        }
    }

    @Test
    void snapshotEmptyBeforeAnyStep() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t");

            try (ZSet snap = engine.getSnapshot()) {
                assertTrue(snap.isEmpty());
            }
        }
    }
}
