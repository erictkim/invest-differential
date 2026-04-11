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

            // Step 1: initial load
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
                    {2, "Bob", "Mouse", 25},
                    {3, "Charlie", "Monitor", 349},
                    {4, "Alice", "Keyboard", 75},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Charlie", "Monitor", 349)));
            }

            // Step 2: add more orders — two qualify, one doesn't
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {5, "Bob", "Headphones", 199},
                    {6, "Diana", "Desk", 450},
                    {7, "Charlie", "Cable", 12},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Bob", "Headphones", 199)));
                assertEquals(1, rows.get(List.of("Diana", "Desk", 450)));
            }

            // Step 3: delete Charlie's Monitor — retracted from view
            ZSet deletion;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{3, "Charlie", "Monitor", 349}})) {
                deletion = src.negate();
            }
            engine.pushChanges("orders", deletion).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Charlie", "Monitor", 349)));
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

            // Step 1: initial load
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Widget", 50},
                    {2, "Gadget", 120},
                    {3, "Widget", 75},
                    {4, "Gadget", 80},
                    {5, "Gizmo", 200},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Widget", 125, 2L)));
                assertEquals(1, rows.get(List.of("Gadget", 200, 2L)));
                assertEquals(1, rows.get(List.of("Gizmo", 200, 1L)));
            }

            // Step 2: more Widget and Gadget sales
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {6, "Widget", 100},
                    {7, "Gadget", 50},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(4, rows.size());
                assertEquals(-1, rows.get(List.of("Widget", 125, 2L)));
                assertEquals(1, rows.get(List.of("Widget", 225, 3L)));
                assertEquals(-1, rows.get(List.of("Gadget", 200, 2L)));
                assertEquals(1, rows.get(List.of("Gadget", 250, 3L)));
            }

            // Step 3: delete a Gizmo sale — group aggregate updates
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{5, "Gizmo", 200}})) {
                del = src.negate();
            }
            engine.pushChanges("sales", del).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Gizmo", 200, 1L)));
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
            // Step 1: initial customers and orders
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

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "Gold", "Laptop", 999)));
                assertEquals(1, rows.get(List.of("Alice", "Gold", "Monitor", 349)));
                assertEquals(1, rows.get(List.of("Bob", "Silver", "Mouse", 25)));
            }

            // Step 2: Charlie places his first order
            engine.pushChanges("orders", ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{
                    {104, 3, "Keyboard", 75},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Charlie", "Bronze", "Keyboard", 75)));
            }

            // Step 3: delete Alice's Monitor order
            ZSet orderDel;
            try (ZSet src = ZSet.fromData(ordersJoinSchema(), allocator, new Object[][]{{103, 1, "Monitor", 349}})) {
                orderDel = src.negate();
            }
            engine.pushChanges("orders", orderDel).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Alice", "Gold", "Monitor", 349)));
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

    // ---- Combined operator variations ----

    /** Join + Filter: only high-value joined orders pass through */
    @Test
    void joinThenFilter_highValueOrders() {
        Schema customers = new Schema(List.of(
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));
        Schema orders = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("item", new ArrowType.Utf8()),
                Field.notNullable("price", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("customers", customers)
                  .registerTable("orders", orders)
                  .sql("SELECT c.name, o.item, o.price " +
                       "FROM orders o JOIN customers c ON o.cust_id = c.cust_id " +
                       "WHERE o.price > 200");

            // Step 1: initial load
            engine.pushChanges("customers", ZSet.fromData(customers, allocator, new Object[][]{
                    {1, "Alice"}, {2, "Bob"},
            }));
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {10, 1, "Phone", 699},
                    {11, 1, "Cable", 15},
                    {12, 2, "TV", 1200},
                    {13, 2, "Pen", 3},
            }));
            engine.step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", "Phone", 699)));
                assertEquals(1, rows.get(List.of("Bob", "TV", 1200)));
            }

            // Step 2: Bob buys a high-value Laptop
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {14, 2, "Laptop", 999},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Bob", "Laptop", 999)));
            }

            // Step 3: delete Alice's Phone — retracted from high-value view
            ZSet phoneDel;
            try (ZSet src = ZSet.fromData(orders, allocator, new Object[][]{{10, 1, "Phone", 699}})) {
                phoneDel = src.negate();
            }
            engine.pushChanges("orders", phoneDel).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Alice", "Phone", 699)));
            }
        }
    }

    /** Join + Aggregate: total spend per customer across orders */
    @Test
    void joinThenAggregate_spendPerCustomer() {
        Schema customers = new Schema(List.of(
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));
        Schema orders = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("customers", customers)
                  .registerTable("orders", orders)
                  .sql("SELECT c.name, SUM(o.amount) as total_spend " +
                       "FROM orders o JOIN customers c ON o.cust_id = c.cust_id " +
                       "GROUP BY c.name");

            engine.pushChanges("customers", ZSet.fromData(customers, allocator, new Object[][]{
                    {1, "Alice"}, {2, "Bob"},
            }));
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {10, 1, 100}, {11, 1, 250}, {12, 2, 75},
            }));
            engine.step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Alice", 350)));
                assertEquals(1, rows.get(List.of("Bob", 75)));
            }

            // Step 2: Bob places another order — aggregate updates
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {13, 2, 225},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Bob", 75)));
                assertEquals(1, rows.get(List.of("Bob", 300)));
            }
        }
    }

    /** Filter + Aggregate: count high-value transactions by category */
    @Test
    void filterThenAggregate_countByCategory() {
        Schema txns = new Schema(List.of(
                Field.notNullable("txn_id", new ArrowType.Int(32, true)),
                Field.notNullable("category", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txns", txns)
                  .sql("SELECT category, COUNT(*) as cnt " +
                       "FROM txns WHERE amount >= 50 GROUP BY category");

            // Step 1: initial load
            engine.pushChanges("txns", ZSet.fromData(txns, allocator, new Object[][]{
                    {1, "Food", 30},
                    {2, "Food", 80},
                    {3, "Transport", 55},
                    {4, "Transport", 20},
                    {5, "Entertainment", 100},
                    {6, "Entertainment", 60},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Food", 1L)));
                assertEquals(1, rows.get(List.of("Transport", 1L)));
                assertEquals(1, rows.get(List.of("Entertainment", 2L)));
            }

            // Step 2: two more Food transactions above threshold
            engine.pushChanges("txns", ZSet.fromData(txns, allocator, new Object[][]{
                    {7, "Food", 65},
                    {8, "Food", 90},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Food", 1L)));
                assertEquals(1, rows.get(List.of("Food", 3L)));
            }

            // Step 3: delete the only qualifying Transport txn
            ZSet deletion;
            try (ZSet src = ZSet.fromData(txns, allocator, new Object[][]{{3, "Transport", 55}})) {
                deletion = src.negate();
            }
            engine.pushChanges("txns", deletion).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Transport", 1L)));
            }
        }
    }

    /** Multi-step aggregation: group disappears when all rows deleted */
    @Test
    void aggregation_groupDisappearsOnFullDelete() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("dept", new ArrowType.Utf8()),
                Field.notNullable("salary", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("employees", schema)
                  .sql("SELECT dept, SUM(salary) as total_salary, COUNT(*) as headcount " +
                       "FROM employees GROUP BY dept");

            // Step 1: Two departments
            engine.pushChanges("employees", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "Engineering", 120000},
                    {2, "Engineering", 95000},
                    {3, "Marketing", 80000},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", 215000, 2L)));
                assertEquals(1, rows.get(List.of("Marketing", 80000, 1L)));
            }

            // Step 2: Delete the only Marketing employee
            ZSet deletion;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{3, "Marketing", 80000}})) {
                deletion = src.negate();
            }
            engine.pushChanges("employees", deletion).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Marketing", 80000, 1L))); // group gone
            }
        }
    }

    /** Project with expression: computed column */
    @Test
    void projectExpression_computedColumn() {
        Schema schema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("price", new ArrowType.Int(32, true)),
                Field.notNullable("qty", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("line_items", schema)
                  .sql("SELECT product, price * qty as total FROM line_items");

            // Step 1: initial load
            engine.pushChanges("line_items", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Bolt", 5, 100},
                    {"Nut", 2, 250},
                    {"Washer", 1, 500},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Bolt", 500)));
                assertEquals(1, rows.get(List.of("Nut", 500)));
                assertEquals(1, rows.get(List.of("Washer", 500)));
            }

            // Step 2: add a new line item
            engine.pushChanges("line_items", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Screw", 3, 200},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("Screw", 600)));
            }

            // Step 3: update Bolt qty from 100 to 300 (delete old, insert new)
            ZSet update;
            try (ZSet del = ZSet.fromData(schema, allocator, new Object[][]{{"Bolt", 5, 100}})) {
                try (ZSet neg = del.negate();
                     ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{{"Bolt", 5, 300}})) {
                    update = neg.add(ins);
                }
            }
            engine.pushChanges("line_items", update).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Bolt", 500)));
                assertEquals(1, rows.get(List.of("Bolt", 1500)));
            }
        }
    }

    /** Three-step join: delete from right side retracts joined rows */
    @Test
    void join_deleteFromRightSide() {
        Schema depts = new Schema(List.of(
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_name", new ArrowType.Utf8())
        ));
        Schema employees = new Schema(List.of(
                Field.notNullable("emp_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("depts", depts)
                  .registerTable("employees", employees)
                  .sql("SELECT d.dept_name, e.name " +
                       "FROM employees e JOIN depts d ON e.dept_id = d.dept_id");

            // Step 1
            engine.pushChanges("depts", ZSet.fromData(depts, allocator, new Object[][]{
                    {10, "Engineering"}, {20, "Sales"},
            }));
            engine.pushChanges("employees", ZSet.fromData(employees, allocator, new Object[][]{
                    {1, 10, "Alice"}, {2, 10, "Bob"}, {3, 20, "Charlie"},
            }));
            engine.step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                assertEquals(3, toMap(delta1).size());
            }

            // Step 2: Delete Bob from Engineering
            ZSet empDel;
            try (ZSet src = ZSet.fromData(employees, allocator, new Object[][]{{2, 10, "Bob"}})) {
                empDel = src.negate();
            }
            engine.pushChanges("employees", empDel).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of("Engineering", "Bob")));
            }
        }
    }

    /** UNION ALL: combine two filtered streams */
    @Test
    void unionAll_combineFilteredStreams() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("origin", new ArrowType.Utf8()),
                Field.notNullable("reading", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("stream_a", schema)
                  .registerTable("stream_b", schema)
                  .sql("SELECT origin, reading FROM stream_a WHERE reading > 10 " +
                       "UNION ALL " +
                       "SELECT origin, reading FROM stream_b WHERE reading > 10");

            // Step 1: initial readings from both streams
            engine.pushChanges("stream_a", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "sensor_1", 15}, {2, "sensor_1", 5},
            }));
            engine.pushChanges("stream_b", ZSet.fromData(schema, allocator, new Object[][]{
                    {3, "sensor_2", 25}, {4, "sensor_2", 8},
            }));
            engine.step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("sensor_1", 15)));
                assertEquals(1, rows.get(List.of("sensor_2", 25)));
            }

            // Step 2: new high reading from stream_a only
            engine.pushChanges("stream_a", ZSet.fromData(schema, allocator, new Object[][]{
                    {5, "sensor_1", 42},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("sensor_1", 42)));
            }

            // Step 3: delete a qualifying reading from stream_b, add new one to stream_b
            ZSet bDel;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{3, "sensor_2", 25}})) {
                bDel = src.negate();
            }
            try (ZSet bIns = ZSet.fromData(schema, allocator, new Object[][]{{6, "sensor_3", 99}})) {
                ZSet combined = bDel.add(bIns);
                bDel.close();
                engine.pushChanges("stream_b", combined).step();
            }

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("sensor_2", 25)));
                assertEquals(1, rows.get(List.of("sensor_3", 99)));
            }
        }
    }

    /** Multiple aggregates: SUM, COUNT, MIN, MAX on same group */
    @Test
    void aggregation_multipleAggFunctions() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("region", new ArrowType.Utf8()),
                Field.notNullable("temp", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("readings", schema)
                  .sql("SELECT region, MIN(temp) as min_temp, MAX(temp) as max_temp, COUNT(*) as readings " +
                       "FROM readings GROUP BY region");

            // Step 1: initial readings
            engine.pushChanges("readings", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "North", 32},
                    {2, "North", 28},
                    {3, "North", 35},
                    {4, "South", 72},
                    {5, "South", 85},
            })).step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("North", 28, 35, 3L)));
                assertEquals(1, rows.get(List.of("South", 72, 85, 2L)));
            }

            // Step 2: new extreme reading in North — updates MIN and MAX
            engine.pushChanges("readings", ZSet.fromData(schema, allocator, new Object[][]{
                    {6, "North", 20},
                    {7, "North", 40},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("North", 28, 35, 3L)));
                assertEquals(1, rows.get(List.of("North", 20, 40, 5L)));
            }

            // Step 3: add a new "West" region
            engine.pushChanges("readings", ZSet.fromData(schema, allocator, new Object[][]{
                    {8, "West", 55},
                    {9, "West", 60},
            })).step();

            try (ZSet delta3 = engine.getOutput()) {
                delta3.compact();
                Map<List<Object>, Integer> rows = toMap(delta3);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of("West", 55, 60, 2L)));
            }
        }
    }

    /** Multi-step filter: item crossing threshold in/out of view */
    @Test
    void filter_itemCrossesThreshold() {
        Schema schema = new Schema(List.of(
                Field.notNullable("sensor_id", new ArrowType.Int(32, true)),
                Field.notNullable("reading", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sensors", schema)
                  .sql("SELECT sensor_id, reading FROM sensors WHERE reading > 100");

            // Step 1: sensor 1 above, sensor 2 below
            engine.pushChanges("sensors", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 150}, {2, 80},
            })).step();

            try (ZSet d1 = engine.getOutput()) {
                d1.compact();
                assertEquals(1, toMap(d1).size());
                assertEquals(1, toMap(d1).get(List.of(1, 150)));
            }

            // Step 2: update sensor 1 to below threshold, sensor 2 to above
            ZSet changes;
            try (ZSet del1 = ZSet.fromData(schema, allocator, new Object[][]{{1, 150}})) {
                try (ZSet neg1 = del1.negate();
                     ZSet ins1 = ZSet.fromData(schema, allocator, new Object[][]{{1, 50}})) {
                    try (ZSet part1 = neg1.add(ins1)) {
                        try (ZSet del2 = ZSet.fromData(schema, allocator, new Object[][]{{2, 80}})) {
                            try (ZSet neg2 = del2.negate();
                                 ZSet ins2 = ZSet.fromData(schema, allocator, new Object[][]{{2, 200}})) {
                                try (ZSet part2 = neg2.add(ins2)) {
                                    changes = part1.add(part2);
                                }
                            }
                        }
                    }
                }
            }
            engine.pushChanges("sensors", changes).step();

            try (ZSet d2 = engine.getOutput()) {
                d2.compact();
                Map<List<Object>, Integer> rows = toMap(d2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of(1, 150))); // sensor 1 leaves view
                assertEquals(1, rows.get(List.of(2, 200)));  // sensor 2 enters view
            }
        }
    }

    /** Join + Filter + Aggregate: total revenue per customer for premium orders */
    @Test
    void joinFilterAggregate_premiumOrderRevenue() {
        Schema customers = new Schema(List.of(
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));
        Schema orders = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("customers", customers)
                  .registerTable("orders", orders)
                  .sql("SELECT c.name, SUM(o.amount) as premium_total, COUNT(*) as premium_count " +
                       "FROM orders o JOIN customers c ON o.cust_id = c.cust_id " +
                       "WHERE o.amount >= 500 " +
                       "GROUP BY c.name");

            engine.pushChanges("customers", ZSet.fromData(customers, allocator, new Object[][]{
                    {1, "Acme Corp"}, {2, "Globex"},
            }));
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {101, 1, 750},   // Acme, premium
                    {102, 1, 100},   // Acme, not premium
                    {103, 2, 1200},  // Globex, premium
                    {104, 2, 600},   // Globex, premium
                    {105, 2, 50},    // Globex, not premium
            }));
            engine.step();

            try (ZSet delta1 = engine.getOutput()) {
                delta1.compact();
                Map<List<Object>, Integer> rows = toMap(delta1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Acme Corp", 750, 1L)));
                assertEquals(1, rows.get(List.of("Globex", 1800, 2L)));
            }

            // Step 2: Acme places another premium order
            engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
                    {106, 1, 900},
            })).step();

            try (ZSet delta2 = engine.getOutput()) {
                delta2.compact();
                Map<List<Object>, Integer> rows = toMap(delta2);
                assertEquals(2, rows.size());
                assertEquals(-1, rows.get(List.of("Acme Corp", 750, 1L)));
                assertEquals(1, rows.get(List.of("Acme Corp", 1650, 2L)));
            }
        }
    }
}
