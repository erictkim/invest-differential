package com.invest.differential;

import com.invest.differential.operator.Circuit;
import com.invest.differential.parallel.ParallelConfig;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that all queries produce identical results whether executed
 * sequentially or with parallel execution enabled.
 * Each test is parameterized: false = sequential, true = parallel.
 */
class ParallelExecutionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private IncrementalEngine createEngine(boolean parallel) {
        IncrementalEngine engine = IncrementalEngine.create(allocator);
        if (parallel) {
            engine.setParallelConfig(ParallelConfig.custom(
                    4,   // 4 threads
                    10,  // low threshold so parallelism kicks in during tests
                    0    // no minimum nanos
            ));
        }
        return engine;
    }

    /** Collect a ZSet into a sorted map for deterministic comparison. */
    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new TreeMap<>(Comparator.comparing(Object::toString));
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        // Remove zero-weight entries
        map.values().removeIf(w -> w == 0);
        return map;
    }

    // ---- Filter ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void filter_initialBatch(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("orders", schema)
                  .sql("SELECT id, name, amount FROM orders WHERE amount > 100");

            Object[][] data = new Object[200][];
            for (int i = 0; i < 200; i++) {
                data[i] = new Object[]{i, "item" + i, i * 3};
            }

            engine.pushChanges("orders", ZSet.fromData(schema, allocator, data)).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                // Items with amount > 100: i*3 > 100 → i > 33, so i from 34..199 = 166 items
                assertEquals(166, rows.size());
                for (var entry : rows.entrySet()) {
                    assertEquals(1, entry.getValue());
                    int amount = (int) entry.getKey().get(2);
                    assertTrue(amount > 100, "amount should be > 100: " + amount);
                }
            }
        }
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void filter_incrementalInsertDelete(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("price", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("products", schema)
                  .sql("SELECT product, price FROM products WHERE price > 50");

            // Step 1: bulk insert
            engine.pushChanges("products", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Laptop", 999}, {"Mouse", 25}, {"Monitor", 349},
                    {"Cable", 12}, {"Keyboard", 75}, {"Headphones", 199}
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(4, rows.size()); // Laptop, Monitor, Keyboard, Headphones
            }

            // Step 2: delete Keyboard
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{"Keyboard", 75}})) {
                del = src.negate();
            }
            engine.pushChanges("products", del).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Keyboard", 75)));
            }

            // Snapshot should have 3 items
            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(3, snap.rowCount());
            }
        }
    }

    // ---- Project ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void project_selectColumns(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("a", new ArrowType.Int(32, true)),
                Field.notNullable("b", new ArrowType.Utf8()),
                Field.notNullable("c", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("t", schema)
                  .sql("SELECT b, c FROM t");

            Object[][] data = new Object[100][];
            for (int i = 0; i < 100; i++) {
                data[i] = new Object[]{i, "val" + i, i * 10};
            }

            engine.pushChanges("t", ZSet.fromData(schema, allocator, data)).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                assertEquals(100, result.rowCount());
            }
        }
    }

    // ---- Join ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void innerJoin_basic(boolean parallel) {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("product_id", new ArrowType.Int(32, true)),
                Field.notNullable("quantity", new ArrowType.Int(32, true))
        ));
        Schema productsSchema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8()),
                Field.notNullable("price", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("orders", ordersSchema)
                  .registerTable("products", productsSchema)
                  .sql("SELECT o.order_id, p.name, o.quantity, p.price " +
                       "FROM orders o JOIN products p ON o.product_id = p.id");

            // Insert products
            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, new Object[][]{
                    {1, "Laptop", 999}, {2, "Mouse", 25}, {3, "Monitor", 349}
            }));

            // Insert orders
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {100, 1, 2},  // 2 laptops
                    {101, 2, 5},  // 5 mice
                    {102, 3, 1},  // 1 monitor
                    {103, 1, 1},  // 1 laptop
            }));
            engine.step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                assertEquals(4, result.rowCount());
            }

            // Step 2: add more orders (incremental)
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {104, 2, 3}  // 3 mice
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                assertEquals(1, result.rowCount()); // only the new join match
            }
        }
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void innerJoin_incrementalBothSides(boolean parallel) {
        Schema left = new Schema(List.of(
                Field.notNullable("key", new ArrowType.Int(32, true)),
                Field.notNullable("lval", new ArrowType.Utf8())
        ));
        Schema right = new Schema(List.of(
                Field.notNullable("key", new ArrowType.Int(32, true)),
                Field.notNullable("rval", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("l", left)
                  .registerTable("r", right)
                  .sql("SELECT l.key, l.lval, r.rval FROM l JOIN r ON l.key = r.key");

            // Step 1: populate both sides
            engine.pushChanges("l", ZSet.fromData(left, allocator, new Object[][]{
                    {1, "a"}, {2, "b"}, {3, "c"}
            }));
            engine.pushChanges("r", ZSet.fromData(right, allocator, new Object[][]{
                    {1, 10}, {2, 20}
            }));
            engine.step();

            try (ZSet r1 = engine.getSnapshot()) {
                r1.compact();
                assertEquals(2, r1.rowCount()); // keys 1 and 2 match
            }

            // Step 2: add right row for key 3
            engine.pushChanges("r", ZSet.fromData(right, allocator, new Object[][]{
                    {3, 30}
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                assertEquals(1, r2.rowCount()); // new join for key 3
            }

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(3, snap.rowCount());
            }
        }
    }

    // ---- Aggregate ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void aggregate_groupBySum(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("category", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT category, SUM(amount) as total FROM sales GROUP BY category");

            // Step 1: initial data
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"electronics", 100}, {"electronics", 200}, {"books", 50},
                    {"books", 30}, {"clothing", 75}
            })).step();

            try (ZSet r1 = engine.getSnapshot()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(3, rows.size());
            }

            // Step 2: add more sales
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"electronics", 300}, {"food", 40}
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(4, snap.rowCount()); // 4 categories now
            }
        }
    }

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void aggregate_countDistinct(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("dept", new ArrowType.Utf8()),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("employees", schema)
                  .sql("SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept");

            engine.pushChanges("employees", ZSet.fromData(schema, allocator, new Object[][]{
                    {"eng", "Alice"}, {"eng", "Bob"}, {"eng", "Charlie"},
                    {"sales", "Diana"}, {"sales", "Eve"}
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(2, snap.rowCount());
            }

            // Delete one employee
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{"eng", "Bob"}})) {
                del = src.negate();
            }
            engine.pushChanges("employees", del).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(2, snap.rowCount()); // still 2 departments
            }
        }
    }

    // ---- Join + Aggregate (complex pipeline) ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void joinThenAggregate(boolean parallel) {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("product_id", new ArrowType.Int(32, true)),
                Field.notNullable("quantity", new ArrowType.Int(32, true))
        ));
        Schema productsSchema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("category", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("orders", ordersSchema)
                  .registerTable("products", productsSchema)
                  .sql("SELECT p.category, SUM(o.quantity) as total_qty " +
                       "FROM orders o JOIN products p ON o.product_id = p.id " +
                       "GROUP BY p.category");

            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, new Object[][]{
                    {1, "electronics"}, {2, "electronics"}, {3, "books"}
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, 10}, {2, 5}, {3, 3}, {1, 2}
            }));
            engine.step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(2, snap.rowCount()); // electronics, books
            }

            // Add more orders
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {3, 7}
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(2, snap.rowCount());
            }
        }
    }

    // ---- Consistency: sequential vs parallel produce same results ----

    @Test
    void consistencyCheck_filterProject() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8()),
                Field.notNullable("score", new ArrowType.Int(32, true))
        ));

        Object[][] data = new Object[500][];
        for (int i = 0; i < 500; i++) {
            data[i] = new Object[]{i, "user" + i, i % 100};
        }

        Map<List<Object>, Integer> seqResult;
        Map<List<Object>, Integer> parResult;

        // Run sequential
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT name, score FROM users WHERE score > 50");
            engine.pushChanges("users", ZSet.fromData(schema, allocator, data)).step();
            try (ZSet result = engine.getSnapshot()) {
                result.compact();
                seqResult = toMap(result);
            }
        }

        // Run parallel
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.setParallelConfig(ParallelConfig.custom(4, 10, 0));
            engine.registerTable("users", schema)
                  .sql("SELECT name, score FROM users WHERE score > 50");
            engine.pushChanges("users", ZSet.fromData(schema, allocator, data)).step();
            try (ZSet result = engine.getSnapshot()) {
                result.compact();
                parResult = toMap(result);
            }
        }

        assertEquals(seqResult, parResult, "Sequential and parallel should produce identical results");
    }

    @Test
    void consistencyCheck_joinAggregate() {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("product_id", new ArrowType.Int(32, true)),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));
        Schema productsSchema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("category", new ArrowType.Utf8())
        ));

        Object[][] products = {
                {1, "A"}, {2, "B"}, {3, "A"}, {4, "C"}, {5, "B"}
        };

        Object[][] orders = new Object[200][];
        for (int i = 0; i < 200; i++) {
            orders[i] = new Object[]{(i % 5) + 1, 10 + i};
        }

        String sql = "SELECT p.category, SUM(o.amount) " +
                     "FROM orders o JOIN products p ON o.product_id = p.id " +
                     "GROUP BY p.category";

        Map<List<Object>, Integer> seqResult;
        Map<List<Object>, Integer> parResult;

        // Sequential
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                  .registerTable("products", productsSchema)
                  .sql(sql);
            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, products));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, orders));
            engine.step();
            try (ZSet result = engine.getSnapshot()) {
                result.compact();
                seqResult = toMap(result);
            }
        }

        // Parallel
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.setParallelConfig(ParallelConfig.custom(4, 10, 0));
            engine.registerTable("orders", ordersSchema)
                  .registerTable("products", productsSchema)
                  .sql(sql);
            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, products));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, orders));
            engine.step();
            try (ZSet result = engine.getSnapshot()) {
                result.compact();
                parResult = toMap(result);
            }
        }

        assertEquals(seqResult, parResult, "Sequential and parallel join+aggregate should produce identical results");
    }

    @Test
    void consistencyCheck_multiStepIncremental() {
        Schema schema = new Schema(List.of(
                Field.notNullable("key", new ArrowType.Int(32, true)),
                Field.notNullable("val", new ArrowType.Int(32, true))
        ));

        String sql = "SELECT key, SUM(val) FROM data GROUP BY key";

        // Generate 5 batches of incremental changes
        Object[][][] batches = new Object[5][][];
        for (int b = 0; b < 5; b++) {
            batches[b] = new Object[50][];
            for (int i = 0; i < 50; i++) {
                batches[b][i] = new Object[]{(b * 50 + i) % 10, b * 50 + i};
            }
        }

        Map<List<Object>, Integer> seqResult;
        Map<List<Object>, Integer> parResult;

        // Sequential
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("data", schema).sql(sql);
            for (Object[][] batch : batches) {
                engine.pushChanges("data", ZSet.fromData(schema, allocator, batch)).step();
            }
            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                seqResult = toMap(snap);
            }
        }

        // Parallel
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.setParallelConfig(ParallelConfig.custom(4, 10, 0));
            engine.registerTable("data", schema).sql(sql);
            for (Object[][] batch : batches) {
                engine.pushChanges("data", ZSet.fromData(schema, allocator, batch)).step();
            }
            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                parResult = toMap(snap);
            }
        }

        assertEquals(seqResult, parResult, "Multi-step incremental results must match");
    }

    // ---- Wavefront DAG scheduling ----

    @Test
    void wavefrontScheduling_multiQuery() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("val", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = createEngine(true)) {
            engine.registerTable("t", schema)
                  .sql("SELECT id, val FROM t WHERE val > 10", "view1")
                  .sql("SELECT id, val FROM t WHERE val < 5", "view2");

            // Both views share input — their filter chains should be in the same wave
            Circuit circuit = engine.getCircuit();
            List<List<com.invest.differential.operator.Operator>> waves = circuit.buildWaves();
            // Should have at least 2 waves: inputs wave and downstream waves
            assertTrue(waves.size() >= 2, "Should have multiple waves");

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 3}, {2, 15}, {3, 1}, {4, 20}
            })).step();

            try (ZSet r1 = engine.getOutput("view1")) {
                r1.compact();
                assertEquals(2, r1.rowCount()); // val > 10: {2,15}, {4,20}
            }
            try (ZSet r2 = engine.getOutput("view2")) {
                r2.compact();
                assertEquals(2, r2.rowCount()); // val < 5: {1,3}, {3,1}
            }
        }
    }

    // ---- Toggle parallelism on/off ----

    @Test
    void toggleParallelism() {
        Schema schema = new Schema(List.of(
                Field.notNullable("x", new ArrowType.Int(32, true)),
                Field.notNullable("y", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                  .sql("SELECT x, SUM(y) FROM t GROUP BY x");

            // Start sequential
            assertFalse(engine.getParallelConfig().isEnabled());

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10}, {2, 20}, {1, 30}
            })).step();

            Map<List<Object>, Integer> seqSnap;
            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                seqSnap = toMap(snap);
            }

            // Turn on parallelism
            engine.setParallel(true);
            assertTrue(engine.getParallelConfig().isEnabled());

            // Add more data with parallelism on
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {2, 40}, {3, 50}
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(3, snap.rowCount()); // 3 groups
            }

            // Turn off parallelism
            engine.setParallel(false);
            assertFalse(engine.getParallelConfig().isEnabled());

            // Continue adding data
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5}
            })).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(3, snap.rowCount()); // still 3 groups
            }
        }
    }

    // ---- ZSet partitioning ----

    @Test
    void zsetHashPartition_roundTrip() {
        Schema schema = new Schema(List.of(
                Field.notNullable("key", new ArrowType.Int(32, true)),
                Field.notNullable("val", new ArrowType.Utf8())
        ));

        try (ZSet original = ZSet.fromData(schema, allocator, new Object[][]{
                {1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"},
                {6, "f"}, {7, "g"}, {8, "h"}, {9, "i"}, {10, "j"}
        })) {
            int[] keyCols = {0};
            ZSet[] parts = original.hashPartition(keyCols, 3);

            // Every row should end up in exactly one partition
            int totalRows = 0;
            for (ZSet p : parts) totalRows += p.rowCount();
            assertEquals(10, totalRows);

            // Concat should reconstruct the original
            ZSet merged = ZSet.concat(parts, schema, allocator);
            merged.compact();
            original.compact();
            assertTrue(original.equalsZSet(merged));

            merged.close();
            for (ZSet p : parts) p.close();
        }
    }

    @Test
    void zsetHashPartition_sameKeySamePartition() {
        Schema schema = new Schema(List.of(
                Field.notNullable("key", new ArrowType.Int(32, true)),
                Field.notNullable("val", new ArrowType.Int(32, true))
        ));

        try (ZSet original = ZSet.fromData(schema, allocator, new Object[][]{
                {1, 10}, {1, 20}, {1, 30}, {2, 40}, {2, 50}
        })) {
            int[] keyCols = {0};
            ZSet[] parts = original.hashPartition(keyCols, 4);

            // All rows with key=1 should be in the same partition
            // All rows with key=2 should be in the same partition
            int key1Part = -1, key2Part = -1;
            for (int i = 0; i < parts.length; i++) {
                for (int row = 0; row < parts[i].rowCount(); row++) {
                    Object[] vals = parts[i].getDataValues(row);
                    int key = (int) vals[0];
                    if (key == 1) {
                        if (key1Part == -1) key1Part = i;
                        else assertEquals(key1Part, i, "All key=1 rows should be in same partition");
                    } else {
                        if (key2Part == -1) key2Part = i;
                        else assertEquals(key2Part, i, "All key=2 rows should be in same partition");
                    }
                }
            }

            for (ZSet p : parts) p.close();
        }
    }

    // ---- Parallel with deletions ----

    @ParameterizedTest(name = "parallel={0}")
    @ValueSource(booleans = {false, true})
    void deletesThenInserts(boolean parallel) {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = createEngine(parallel)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name FROM users");

            // Insert 100 rows
            Object[][] data = new Object[100][];
            for (int i = 0; i < 100; i++) data[i] = new Object[]{i, "user" + i};
            engine.pushChanges("users", ZSet.fromData(schema, allocator, data)).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(100, snap.rowCount());
            }

            // Delete 50 rows
            Object[][] delData = new Object[50][];
            for (int i = 0; i < 50; i++) delData[i] = new Object[]{i, "user" + i};
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, delData)) {
                del = src.negate();
            }
            engine.pushChanges("users", del).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(50, snap.rowCount());
            }

            // Insert 25 new rows
            Object[][] newData = new Object[25][];
            for (int i = 0; i < 25; i++) newData[i] = new Object[]{200 + i, "newuser" + i};
            engine.pushChanges("users", ZSet.fromData(schema, allocator, newData)).step();

            try (ZSet snap = engine.getSnapshot()) {
                snap.compact();
                assertEquals(75, snap.rowCount());
            }
        }
    }

    // ---- ParallelConfig API ----

    @Test
    void parallelConfig_disabled() {
        ParallelConfig config = ParallelConfig.disabled();
        assertFalse(config.isEnabled());
        assertEquals(1, config.getMaxParallelism());
        assertNull(config.getPool());
    }

    @Test
    void parallelConfig_withDefaults() {
        ParallelConfig config = ParallelConfig.withDefaults();
        assertTrue(config.isEnabled());
        assertTrue(config.getMaxParallelism() >= 1);
        assertNotNull(config.getPool());
        config.shutdown();
    }

    @Test
    void parallelConfig_custom() {
        ParallelConfig config = ParallelConfig.custom(8, 1000, 50_000);
        assertTrue(config.isEnabled());
        assertEquals(8, config.getMaxParallelism());
        assertEquals(1000, config.getMinRowsForDataParallel());
        assertEquals(50_000, config.getMinNanosForAdaptive());
        config.shutdown();
    }
}
