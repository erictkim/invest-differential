package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DemoTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /** Collect a ZSet into a map of data-row -> weight for easy assertion. */
    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }

    // ---- Filter + Project ----

    @Test
    void filterProject_initialBatch() {
        Schema schema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                  .sql("SELECT customer, product, amount FROM orders WHERE amount > 100");

            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
                    {2, "Bob", "Mouse", 25},
                    {3, "Charlie", "Monitor", 349},
                    {4, "Alice", "Keyboard", 75},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Charlie", "Monitor", 349)));
            }
        }
    }

    @Test
    void filterProject_incrementalInsert() {
        Schema schema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                  .sql("SELECT customer, product, amount FROM orders WHERE amount > 100");

            // Step 1
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
                    {2, "Bob", "Mouse", 25},
            })).step();
            engine.getOutput().close();

            // Step 2 — only the new qualifying row appears
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {5, "Bob", "Headphones", 199},
                    {6, "Charlie", "Webcam", 45},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Bob", "Headphones", 199)));
            }
        }
    }

    @Test
    void filterProject_deleteAndReplace() {
        Schema schema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                  .sql("SELECT customer, product, amount FROM orders WHERE amount > 100");

            // Step 1
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
            })).step();
            engine.getOutput().close();

            // Step 2 — delete Laptop, insert Laptop Pro
            ZSet combined;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{1, "Alice", "Laptop", 999}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{7, "Alice", "Laptop Pro", 1299}})) {
                    combined = neg.add(ins);
                }
            }
            engine.pushChanges("orders", combined).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Alice", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Alice", "Laptop Pro", 1299)));
            }
        }
    }

    @Test
    void filterProject_emptyDelta() {
        Schema schema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                  .sql("SELECT customer, product, amount FROM orders WHERE amount > 100");

            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
            })).step();
            engine.getOutput().close();

            // Empty step
            engine.pushChanges("orders", ZSet.empty(schema, allocator)).step();
            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                assertTrue(delta.isEmpty());
            }
        }
    }

    // ---- Aggregation ----

    @Test
    void aggregation_initialGroups() {
        Schema schema = new Schema(List.of(
                Field.notNullable("sale_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT product, SUM(revenue) as total_revenue, COUNT(*) as num_sales " +
                       "FROM sales GROUP BY product");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Widget", 50},
                    {2, "Gadget", 120},
                    {3, "Widget", 75},
                    {4, "Gadget", 80},
                    {5, "Gizmo", 200},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Widget", 125, 2L)));
                assertEquals(1, rows.get(List.of("Gadget", 200, 2L)));
                assertEquals(1, rows.get(List.of("Gizmo", 200, 1L)));
            }
        }
    }

    @Test
    void aggregation_incrementalUpdate() {
        Schema schema = new Schema(List.of(
                Field.notNullable("sale_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT product, SUM(revenue) as total_revenue, COUNT(*) as num_sales " +
                       "FROM sales GROUP BY product");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Widget", 50},
                    {2, "Widget", 75},
            })).step();
            engine.getOutput().close();

            // Step 2 — more Widget sales
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {6, "Widget", 60},
                    {7, "Widget", 90},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Widget", 125, 2L)));  // old aggregate retracted
                assertEquals(1, rows.get(List.of("Widget", 275, 4L)));   // new aggregate
            }
        }
    }

    @Test
    void aggregation_correction() {
        Schema schema = new Schema(List.of(
                Field.notNullable("sale_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT product, SUM(revenue) as total_revenue, COUNT(*) as num_sales " +
                       "FROM sales GROUP BY product");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {5, "Gizmo", 200},
            })).step();
            engine.getOutput().close();

            // Step 2 — correct Gizmo revenue $200 → $250
            ZSet correction;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{5, "Gizmo", 200}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{5, "Gizmo", 250}})) {
                    correction = neg.add(ins);
                }
            }
            engine.pushChanges("sales", correction).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Gizmo", 200, 1L)));
                assertEquals(1, rows.get(List.of("Gizmo", 250, 1L)));
            }
        }
    }

    @Test
    void aggregation_newGroup() {
        Schema schema = new Schema(List.of(
                Field.notNullable("sale_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT product, SUM(revenue) as total_revenue, COUNT(*) as num_sales " +
                       "FROM sales GROUP BY product");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Gadget", 120},
                    {2, "Gadget", 80},
            })).step();
            engine.getOutput().close();

            // Step 2 — Doohickey appears + more Gadget
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {8, "Doohickey", 30},
                    {9, "Doohickey", 45},
                    {10, "Gadget", 150},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Doohickey", 75, 2L)));
                assertEquals(-1, rows.get(List.of("Gadget", 200, 2L)));
                assertEquals(1, rows.get(List.of("Gadget", 350, 3L)));
            }
        }
    }

    // ---- Join ----

    private Schema customersSchema() {
        return new Schema(List.of(
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8()),
                Field.notNullable("tier", new ArrowType.Utf8())
        ));
    }

    private Schema ordersJoinSchema() {
        return new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));
    }

    private IncrementalEngine createJoinEngine() {
        IncrementalEngine engine = IncrementalEngine.create(allocator);
        engine.registerTable("customers", customersSchema())
              .registerTable("orders", ordersJoinSchema())
              .sql("SELECT c.name, c.tier, o.product, o.amount " +
                   "FROM orders o JOIN customers c ON o.cust_id = c.cust_id");
        return engine;
    }

    @Test
    void join_initialLoad() {
        try (IncrementalEngine engine = createJoinEngine()) {
            engine.pushChanges("customers", ZSet.fromData(customersSchema(), allocator, new Object[][]{
                    {1, "Alice", "Gold"},
                    {2, "Bob", "Silver"},
                    {3, "Charlie", "Bronze"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {101, 1, "Laptop", 999},
                    {102, 2, "Mouse", 25},
                    {103, 1, "Monitor", 349},
            }));
            engine.step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "Gold", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Alice", "Gold", "Monitor", 349)));
                assertEquals(1, rows.get(List.of("Bob", "Silver", "Mouse", 25)));
            }
        }
    }

    @Test
    void join_newOrderForExistingCustomer() {
        try (IncrementalEngine engine = createJoinEngine()) {
            // Step 1
            engine.pushChanges("customers", ZSet.fromData(customersSchema(), allocator, new Object[][]{
                    {1, "Alice", "Gold"},
                    {2, "Bob", "Silver"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {101, 1, "Laptop", 999},
            }));
            engine.step();
            engine.getOutput().close();

            // Step 2 — only Bob's new order should appear
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {104, 2, "Keyboard", 75},
            })).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Bob", "Silver", "Keyboard", 75)));
            }
        }
    }

    @Test
    void join_customerUpdate() {
        try (IncrementalEngine engine = createJoinEngine()) {
            // Step 1
            engine.pushChanges("customers", ZSet.fromData(customersSchema(), allocator, new Object[][]{
                    {1, "Alice", "Gold"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {101, 1, "Laptop", 999},
                    {103, 1, "Monitor", 349},
            }));
            engine.step();
            engine.getOutput().close();

            // Step 2 — upgrade Alice Gold → Platinum
            ZSet tierChange;
            try (ZSet old = ZSet.fromData(customersSchema(), allocator, new Object[][]{{1, "Alice", "Gold"}})) {
                try (ZSet neg = old.negate();
                     ZSet upd = ZSet.fromData(customersSchema(), allocator, new Object[][]{{1, "Alice", "Platinum"}})) {
                    tierChange = neg.add(upd);
                }
            }
            engine.pushChanges("customers", tierChange).step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(4, rows.size());
                // Old Gold rows retracted
                assertEquals(-1, rows.get(List.of("Alice", "Gold", "Laptop", 999)));
                assertEquals(-1, rows.get(List.of("Alice", "Gold", "Monitor", 349)));
                // New Platinum rows added
                assertEquals(1, rows.get(List.of("Alice", "Platinum", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Alice", "Platinum", "Monitor", 349)));
            }
        }
    }

    @Test
    void join_newCustomerWithOrder() {
        try (IncrementalEngine engine = createJoinEngine()) {
            // Step 1
            engine.pushChanges("customers", ZSet.fromData(customersSchema(), allocator, new Object[][]{
                    {1, "Alice", "Gold"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {101, 1, "Laptop", 999},
            }));
            engine.step();
            engine.getOutput().close();

            // Step 2 — new customer + order in same step
            engine.pushChanges("customers", ZSet.fromData(customersSchema(), allocator, new Object[][]{
                    {4, "Diana", "Gold"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {105, 4, "Tablet", 499},
            }));
            engine.step();

            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                Map<List<Object>, Integer> rows = toMap(delta);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Diana", "Gold", "Tablet", 499)));
            }
        }
    }
}
