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

class MultiQueryTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void twoFiltersOverSameTable() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100")
                    .sql("SELECT product, amount FROM orders WHERE amount < 50");

            assertEquals(2, engine.getOutputCount());

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"widget", 30},
                    {"gadget", 150},
                    {"thing", 200},
                    {"tiny", 10}
            });
            engine.pushChanges("orders", delta).step();

            try (ZSet high = engine.getOutput(0);
                 ZSet low = engine.getOutput(1)) {
                high.compact();
                low.compact();
                assertEquals(2, high.rowCount()); // gadget=150, thing=200
                assertEquals(2, low.rowCount());  // widget=30, tiny=10
            }
        }
    }

    @Test
    void namedViewAccess() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "expensive")
                    .sql("SELECT product, amount FROM orders WHERE amount <= 100", "cheap");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"widget", 50},
                    {"gadget", 150}
            });
            engine.pushChanges("orders", delta).step();

            try (ZSet expensive = engine.getOutput("expensive");
                 ZSet cheap = engine.getOutput("cheap")) {
                expensive.compact();
                cheap.compact();
                assertEquals(1, expensive.rowCount());
                assertEquals(1, cheap.rowCount());
            }
        }
    }

    @Test
    void namedSnapshotAccess() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "high")
                    .sql("SELECT product, amount FROM orders WHERE amount <= 100", "low");

            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"widget", 50}, {"gadget", 150}
            });
            engine.pushChanges("orders", delta1).step();

            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"thing", 200}, {"tiny", 10}
            });
            engine.pushChanges("orders", delta2).step();

            try (ZSet highSnap = engine.getSnapshot("high");
                 ZSet lowSnap = engine.getSnapshot("low")) {
                highSnap.compact();
                lowSnap.compact();
                assertEquals(2, highSnap.rowCount()); // gadget + thing
                assertEquals(2, lowSnap.rowCount());  // widget + tiny
            }
        }
    }

    @Test
    void filterAndAggregateOverSameTable() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "filtered")
                    .sql("SELECT product, SUM(amount) AS total FROM orders GROUP BY product", "totals");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 50},
                    {"A", 150},
                    {"B", 200}
            });
            engine.pushChanges("orders", delta).step();

            try (ZSet filtered = engine.getOutput("filtered");
                 ZSet totals = engine.getOutput("totals")) {
                filtered.compact();
                totals.compact();
                assertEquals(2, filtered.rowCount()); // A=150, B=200
                assertEquals(2, totals.rowCount());   // A=200, B=200
            }
        }
    }

    @Test
    void incrementalUpdatesPropagateToAllViews() {
        Schema schema = new Schema(List.of(
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("items", schema)
                    .sql("SELECT category, val FROM items WHERE val > 10", "big")
                    .sql("SELECT category, SUM(val) AS total FROM items GROUP BY category", "sums");

            // Step 1
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"X", 5}, {"X", 20}, {"Y", 30}
            });
            engine.pushChanges("items", delta1).step();

            try (ZSet big1 = engine.getSnapshot("big");
                 ZSet sums1 = engine.getSnapshot("sums")) {
                big1.compact();
                sums1.compact();
                assertEquals(2, big1.rowCount());  // X=20, Y=30
                assertEquals(2, sums1.rowCount()); // X=25, Y=30
            }

            // Step 2: add more data
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Y", 100}
            });
            engine.pushChanges("items", delta2).step();

            try (ZSet big2 = engine.getSnapshot("big");
                 ZSet sums2 = engine.getSnapshot("sums")) {
                big2.compact();
                sums2.compact();
                assertEquals(3, big2.rowCount());  // X=20, Y=30, Y=100
                assertEquals(2, sums2.rowCount()); // X=25, Y=130
            }
        }
    }

    @Test
    void twoTablesSharedByTwoQueries() {
        Schema ordersSchema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        Schema productsSchema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                    .registerTable("products", productsSchema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "big_orders")
                    .sql("SELECT name, price FROM products WHERE price > 50", "expensive_products");

            ZSet ordersDelta = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {"widget", 50}, {"gadget", 150}
            });
            ZSet productsDelta = ZSet.fromData(productsSchema, allocator, new Object[][]{
                    {"phone", 999}, {"pen", 2}
            });

            engine.pushChanges("orders", ordersDelta)
                    .pushChanges("products", productsDelta)
                    .step();

            try (ZSet bigOrders = engine.getOutput("big_orders");
                 ZSet expProducts = engine.getOutput("expensive_products")) {
                bigOrders.compact();
                expProducts.compact();
                assertEquals(1, bigOrders.rowCount());   // gadget=150
                assertEquals(1, expProducts.rowCount()); // phone=999
            }
        }
    }

    @Test
    void unknownViewNameThrows() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t", "my_view");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{{1}});
            engine.pushChanges("t", delta).step();

            assertThrows(IllegalArgumentException.class, () -> engine.getOutput("nonexistent"));
        }
    }

    @Test
    void sharedInputOperatorCount() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t WHERE v > 0")
                    .sql("SELECT v FROM t WHERE v < 100");

            // Two queries, same table — should share one InputOperator
            assertEquals(1, engine.getCircuit().getInputs().size());
            assertEquals(2, engine.getOutputCount());
        }
    }

    @Test
    void viewReferencesAnotherView() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "expensive")
                    .sql("SELECT product, amount FROM expensive WHERE amount < 500", "mid_range");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"widget", 50},
                    {"gadget", 150},
                    {"laptop", 999},
                    {"phone", 300}
            });
            engine.pushChanges("orders", delta).step();

            try (ZSet expensive = engine.getOutput("expensive");
                 ZSet midRange = engine.getOutput("mid_range")) {
                expensive.compact();
                midRange.compact();
                assertEquals(3, expensive.rowCount()); // gadget=150, laptop=999, phone=300
                assertEquals(2, midRange.rowCount());  // gadget=150, phone=300
            }
        }
    }

    @Test
    void viewChainThreeLevels() {
        Schema schema = new Schema(List.of(
                new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT x FROM t WHERE x > 0", "positive")
                    .sql("SELECT x FROM positive WHERE x < 100", "small_positive")
                    .sql("SELECT x FROM small_positive WHERE x > 10", "medium");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {-5}, {3}, {15}, {50}, {150}
            });
            engine.pushChanges("t", delta).step();

            try (ZSet pos = engine.getOutput("positive");
                 ZSet small = engine.getOutput("small_positive");
                 ZSet med = engine.getOutput("medium")) {
                pos.compact();
                small.compact();
                med.compact();
                assertEquals(4, pos.rowCount());   // 3, 15, 50, 150
                assertEquals(3, small.rowCount()); // 3, 15, 50
                assertEquals(2, med.rowCount());   // 15, 50
            }
        }
    }

    @Test
    void viewReferenceWithAggregation() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, SUM(amount) AS total FROM orders GROUP BY product", "totals")
                    .sql("SELECT product, total FROM totals WHERE total > 100", "big_totals");

            ZSet delta = ZSet.fromData(schema, allocator, new Object[][]{
                    {"A", 50},
                    {"A", 80},
                    {"B", 30}
            });
            engine.pushChanges("orders", delta).step();

            try (ZSet totals = engine.getOutput("totals");
                 ZSet bigTotals = engine.getOutput("big_totals")) {
                totals.compact();
                bigTotals.compact();
                assertEquals(2, totals.rowCount());    // A=130, B=30
                assertEquals(1, bigTotals.rowCount()); // A=130
            }
        }
    }

    @Test
    void viewReferenceIncrementalUpdates() {
        Schema schema = new Schema(List.of(
                new Field("product", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "expensive")
                    .sql("SELECT product, amount FROM expensive WHERE amount < 500", "mid_range");

            // Step 1
            ZSet delta1 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"gadget", 150}, {"laptop", 999}
            });
            engine.pushChanges("orders", delta1).step();

            try (ZSet snap1 = engine.getSnapshot("mid_range")) {
                snap1.compact();
                assertEquals(1, snap1.rowCount()); // gadget=150
            }

            // Step 2: add another mid-range item
            ZSet delta2 = ZSet.fromData(schema, allocator, new Object[][]{
                    {"phone", 300}
            });
            engine.pushChanges("orders", delta2).step();

            try (ZSet snap2 = engine.getSnapshot("mid_range")) {
                snap2.compact();
                assertEquals(2, snap2.rowCount()); // gadget + phone
            }
        }
    }

    @Test
    void viewReferenceNoInputOperatorCreated() {
        Schema schema = new Schema(List.of(
                new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT v FROM t WHERE v > 0", "pos")
                    .sql("SELECT v FROM pos WHERE v < 100");

            // "pos" is a view reference, not a table — only 1 InputOperator for "t"
            assertEquals(1, engine.getCircuit().getInputs().size());
            assertEquals(2, engine.getOutputCount());
        }
    }

    // ---- Helper ----

    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }

    // ---- Complex view-reference tests ----

    /**
     * Two filtered views (domestic/international orders) joined together
     * to produce a combined report. 3 steps with delta + snapshot checks.
     */
    @Test
    void joinTwoViewsTogether() {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("region", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));
        Schema customersSchema = new Schema(List.of(
                Field.notNullable("region", new ArrowType.Utf8()),
                Field.notNullable("tax_rate", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                    .registerTable("tax_rates", customersSchema)
                    // View 1: only high-value orders
                    .sql("SELECT order_id, region, product, amount FROM orders WHERE amount > 100",
                            "big_orders")
                    // View 2: join big orders with tax rates
                    .sql("SELECT b.order_id, b.product, b.amount, t.tax_rate " +
                            "FROM big_orders b JOIN tax_rates t ON b.region = t.region",
                            "taxed_orders");

            assertEquals(2, engine.getOutputCount());
            // Only 2 InputOperators: orders + tax_rates (big_orders is a view)
            assertEquals(2, engine.getCircuit().getInputs().size());

            // Step 1: initial load
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, "US", "Laptop", 999},
                    {2, "EU", "Mouse", 25},     // filtered out (amount <= 100)
                    {3, "US", "Monitor", 349},
            }));
            engine.pushChanges("tax_rates", ZSet.fromData(customersSchema, allocator, new Object[][]{
                    {"US", 8},
                    {"EU", 20},
            }));
            engine.step();

            try (ZSet bigDelta = engine.getOutput("big_orders");
                 ZSet taxedDelta = engine.getOutput("taxed_orders")) {
                bigDelta.compact();
                taxedDelta.compact();
                assertEquals(2, bigDelta.rowCount());  // Laptop=999, Monitor=349
                Map<List<Object>, Integer> taxed = toMap(taxedDelta);
                assertEquals(2, taxed.size());
                assertEquals(1, taxed.get(List.of(1, "Laptop", 999, 8)));   // US tax
                assertEquals(1, taxed.get(List.of(3, "Monitor", 349, 8)));  // US tax
            }

            // Step 2: add EU big order → joins with EU tax rate
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {4, "EU", "Server", 5000},
            })).step();

            try (ZSet taxedDelta2 = engine.getOutput("taxed_orders")) {
                taxedDelta2.compact();
                Map<List<Object>, Integer> rows = toMap(taxedDelta2);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(4, "Server", 5000, 20))); // EU tax
            }

            try (ZSet taxedSnap = engine.getSnapshot("taxed_orders")) {
                taxedSnap.compact();
                assertEquals(3, taxedSnap.rowCount()); // Laptop + Monitor + Server
            }

            // Step 3: delete US Monitor order via negation
            ZSet del;
            try (ZSet src = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {3, "US", "Monitor", 349}
            })) { del = src.negate(); }
            engine.pushChanges("orders", del).step();

            try (ZSet taxedDelta3 = engine.getOutput("taxed_orders")) {
                taxedDelta3.compact();
                Map<List<Object>, Integer> rows = toMap(taxedDelta3);
                assertEquals(1, rows.size());
                assertEquals(-1, rows.get(List.of(3, "Monitor", 349, 8))); // retracted
            }

            try (ZSet taxedSnap = engine.getSnapshot("taxed_orders")) {
                taxedSnap.compact();
                assertEquals(2, taxedSnap.rowCount()); // Laptop + Server
            }
        }
    }

    /**
     * View 1: filtered orders. View 2: aggregate of view 1.
     * 3 steps checking both delta and snapshot at each.
     */
    @Test
    void aggregateOverFilteredView() {
        Schema schema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("category", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                    .sql("SELECT product, category, amount FROM sales WHERE amount > 50",
                            "big_sales")
                    .sql("SELECT category, SUM(amount) AS total FROM big_sales GROUP BY category",
                            "category_totals");

            // Step 1: initial batch
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Laptop", "Electronics", 999},
                    {"Pen", "Office", 2},          // filtered out
                    {"Monitor", "Electronics", 349},
                    {"Chair", "Office", 150},
            })).step();

            try (ZSet bigDelta1 = engine.getOutput("big_sales")) {
                bigDelta1.compact();
                assertEquals(3, bigDelta1.rowCount()); // Laptop, Monitor, Chair
            }
            try (ZSet totDelta1 = engine.getOutput("category_totals")) {
                totDelta1.compact();
                Map<List<Object>, Integer> rows = toMap(totDelta1);
                assertEquals(1, rows.get(List.of("Electronics", 1348))); // 999+349
                assertEquals(1, rows.get(List.of("Office", 150)));
            }

            // Step 2: add more electronics
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Keyboard", "Electronics", 75},
                    {"Stapler", "Office", 10},   // filtered out
            })).step();

            try (ZSet bigDelta2 = engine.getOutput("big_sales")) {
                bigDelta2.compact();
                assertEquals(1, bigDelta2.rowCount()); // only Keyboard=75
            }
            try (ZSet totSnap2 = engine.getSnapshot("category_totals")) {
                totSnap2.compact();
                Map<List<Object>, Integer> rows = toMap(totSnap2);
                assertEquals(1, rows.get(List.of("Electronics", 1423))); // 1348+75
                assertEquals(1, rows.get(List.of("Office", 150)));       // unchanged
            }

            // Step 3: delete Laptop
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Laptop", "Electronics", 999}
            })) { del = src.negate(); }
            engine.pushChanges("sales", del).step();

            try (ZSet bigSnap3 = engine.getSnapshot("big_sales")) {
                bigSnap3.compact();
                assertEquals(3, bigSnap3.rowCount()); // Monitor, Chair, Keyboard
            }
            try (ZSet totSnap3 = engine.getSnapshot("category_totals")) {
                totSnap3.compact();
                Map<List<Object>, Integer> rows = toMap(totSnap3);
                assertEquals(1, rows.get(List.of("Electronics", 424))); // 349+75
                assertEquals(1, rows.get(List.of("Office", 150)));
            }
        }
    }

    /**
     * Diamond pattern: two independent views from the same table, then joined.
     * orders → big_orders (filter > 100)
     * orders → small_orders (filter <= 100)
     * big_orders JOIN small_orders ON product → combined
     * 3 steps with deltas and snapshots checked.
     */
    @Test
    void diamondJoinTwoViewsFromSameTable() {
        Schema schema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", schema)
                    .sql("SELECT product, amount FROM orders WHERE amount > 100", "big")
                    .sql("SELECT product, amount FROM orders WHERE amount <= 100", "small")
                    .sql("SELECT b.product, b.amount AS big_amount, s.amount AS small_amount " +
                            "FROM big b JOIN small s ON b.product = s.product",
                            "combined");

            // Only 1 InputOperator — all 3 queries share the same "orders" table
            assertEquals(1, engine.getCircuit().getInputs().size());
            assertEquals(3, engine.getOutputCount());

            // Step 1: Widget has both big and small orders → join produces result
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Widget", 200},  // big
                    {"Widget", 30},   // small
                    {"Gadget", 150},  // big only, no small match
                    {"Pen", 5},       // small only, no big match
            })).step();

            try (ZSet bigDel = engine.getOutput("big");
                 ZSet smallDel = engine.getOutput("small");
                 ZSet combDel = engine.getOutput("combined")) {
                bigDel.compact();
                smallDel.compact();
                combDel.compact();
                assertEquals(2, bigDel.rowCount());   // Widget=200, Gadget=150
                assertEquals(2, smallDel.rowCount()); // Widget=30, Pen=5
                // Join: only Widget matches both sides
                Map<List<Object>, Integer> comb = toMap(combDel);
                assertEquals(1, comb.size());
                assertEquals(1, comb.get(List.of("Widget", 200, 30)));
            }

            // Step 2: Gadget gets a small order → now it joins too
            engine.pushChanges("orders", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Gadget", 20},  // small
            })).step();

            try (ZSet combDel2 = engine.getOutput("combined")) {
                combDel2.compact();
                Map<List<Object>, Integer> comb = toMap(combDel2);
                assertEquals(1, comb.size());
                assertEquals(1, comb.get(List.of("Gadget", 150, 20)));
            }

            try (ZSet combSnap2 = engine.getSnapshot("combined")) {
                combSnap2.compact();
                assertEquals(2, combSnap2.rowCount()); // Widget + Gadget
            }

            // Step 3: delete Widget big order → retract from combined
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Widget", 200}
            })) { del = src.negate(); }
            engine.pushChanges("orders", del).step();

            try (ZSet combDel3 = engine.getOutput("combined")) {
                combDel3.compact();
                Map<List<Object>, Integer> comb = toMap(combDel3);
                assertEquals(1, comb.size());
                assertEquals(-1, comb.get(List.of("Widget", 200, 30))); // retracted
            }

            try (ZSet combSnap3 = engine.getSnapshot("combined")) {
                combSnap3.compact();
                assertEquals(1, combSnap3.rowCount()); // only Gadget remains
                Map<List<Object>, Integer> snap = toMap(combSnap3);
                assertEquals(1, snap.get(List.of("Gadget", 150, 20)));
            }
        }
    }

    /**
     * 3-layer pipeline: raw → filtered → aggregated → filtered again.
     * Tests deep chaining with 3 incremental steps.
     */
    @Test
    void threeLayerPipeline() {
        Schema schema = new Schema(List.of(
                Field.notNullable("dept", new ArrowType.Utf8()),
                Field.notNullable("emp", new ArrowType.Utf8()),
                Field.notNullable("salary", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("employees", schema)
                    // Layer 1: only salaries above threshold
                    .sql("SELECT dept, emp, salary FROM employees WHERE salary > 50000",
                            "well_paid")
                    // Layer 2: aggregate well_paid by dept
                    .sql("SELECT dept, SUM(salary) AS total_salary, COUNT(*) AS headcount " +
                            "FROM well_paid GROUP BY dept",
                            "dept_stats")
                    // Layer 3: filter to only departments with 2+ well-paid employees
                    .sql("SELECT dept, total_salary, headcount FROM dept_stats WHERE headcount >= 2",
                            "big_depts");

            // Step 1: initial employees
            engine.pushChanges("employees", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120000},
                    {"Eng", "Bob", 80000},
                    {"Sales", "Charlie", 60000},
                    {"Sales", "Diana", 40000},  // filtered out (< 50000)
                    {"HR", "Eve", 70000},
            })).step();

            // well_paid: Alice, Bob, Charlie, Eve (4 rows)
            try (ZSet wpDelta = engine.getOutput("well_paid")) {
                wpDelta.compact();
                assertEquals(4, wpDelta.rowCount());
            }
            // dept_stats: Eng(200000,2), Sales(60000,1), HR(70000,1)
            try (ZSet dsDelta = engine.getOutput("dept_stats")) {
                dsDelta.compact();
                Map<List<Object>, Integer> rows = toMap(dsDelta);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Eng", 200000, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 60000, 1L)));
                assertEquals(1, rows.get(List.of("HR", 70000, 1L)));
            }
            // big_depts: only Eng (headcount >= 2)
            try (ZSet bdDelta = engine.getOutput("big_depts")) {
                bdDelta.compact();
                assertEquals(1, bdDelta.rowCount());
                Map<List<Object>, Integer> rows = toMap(bdDelta);
                assertEquals(1, rows.get(List.of("Eng", 200000, 2L)));
            }

            // Step 2: add a well-paid Sales employee → Sales becomes a big dept
            engine.pushChanges("employees", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Sales", "Frank", 90000},
            })).step();

            try (ZSet dsSnap2 = engine.getSnapshot("dept_stats")) {
                dsSnap2.compact();
                Map<List<Object>, Integer> rows = toMap(dsSnap2);
                assertEquals(1, rows.get(List.of("Eng", 200000, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 150000, 2L))); // 60000+90000
                assertEquals(1, rows.get(List.of("HR", 70000, 1L)));
            }
            try (ZSet bdSnap2 = engine.getSnapshot("big_depts")) {
                bdSnap2.compact();
                assertEquals(2, bdSnap2.rowCount()); // Eng + Sales
            }

            // Step 3: remove Bob from Eng → Eng drops below 2 headcount
            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Bob", 80000}
            })) { del = src.negate(); }
            engine.pushChanges("employees", del).step();

            try (ZSet dsSnap3 = engine.getSnapshot("dept_stats")) {
                dsSnap3.compact();
                Map<List<Object>, Integer> rows = toMap(dsSnap3);
                assertEquals(1, rows.get(List.of("Eng", 120000, 1L)));   // only Alice now
                assertEquals(1, rows.get(List.of("Sales", 150000, 2L)));
                assertEquals(1, rows.get(List.of("HR", 70000, 1L)));
            }
            try (ZSet bdSnap3 = engine.getSnapshot("big_depts")) {
                bdSnap3.compact();
                assertEquals(1, bdSnap3.rowCount()); // only Sales remains
                Map<List<Object>, Integer> rows = toMap(bdSnap3);
                assertEquals(1, rows.get(List.of("Sales", 150000, 2L)));
            }
        }
    }

    /**
     * Two base tables, two views, joined into a third view.
     * Products (filtered) + orders (aggregated by product) → joined report.
     * 3 steps with delta/snapshot verification.
     */
    @Test
    void joinFilteredTableWithAggregatedView() {
        Schema productsSchema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("category", new ArrowType.Utf8()),
                Field.notNullable("price", new ArrowType.Int(32, true))
        ));
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("qty", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("products", productsSchema)
                    .registerTable("orders", ordersSchema)
                    // View 1: aggregate orders by product
                    .sql("SELECT product, SUM(qty) AS total_qty FROM orders GROUP BY product",
                            "order_totals")
                    // View 2: premium products only
                    .sql("SELECT product, category, price FROM products WHERE price > 100",
                            "premium")
                    // View 3: join premium products with their total orders
                    .sql("SELECT p.product, p.category, p.price, o.total_qty " +
                            "FROM premium p JOIN order_totals o ON p.product = o.product",
                            "premium_report");

            // Step 1: initial load
            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, new Object[][]{
                    {"Laptop", "Electronics", 999},
                    {"Pen", "Office", 2},           // filtered out
                    {"Monitor", "Electronics", 349},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {"Laptop", 5},
                    {"Laptop", 3},
                    {"Pen", 100},
                    {"Monitor", 2},
            })).step();

            try (ZSet otDelta = engine.getOutput("order_totals")) {
                otDelta.compact();
                Map<List<Object>, Integer> rows = toMap(otDelta);
                assertEquals(1, rows.get(List.of("Laptop", 8)));
                assertEquals(1, rows.get(List.of("Pen", 100)));
                assertEquals(1, rows.get(List.of("Monitor", 2)));
            }
            try (ZSet premDelta = engine.getOutput("premium")) {
                premDelta.compact();
                assertEquals(2, premDelta.rowCount()); // Laptop, Monitor
            }
            try (ZSet repDelta = engine.getOutput("premium_report")) {
                repDelta.compact();
                Map<List<Object>, Integer> rows = toMap(repDelta);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Laptop", "Electronics", 999, 8)));
                assertEquals(1, rows.get(List.of("Monitor", "Electronics", 349, 2)));
            }

            // Step 2: more Laptop orders
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {"Laptop", 10},
            })).step();

            try (ZSet repSnap2 = engine.getSnapshot("premium_report")) {
                repSnap2.compact();
                Map<List<Object>, Integer> rows = toMap(repSnap2);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Laptop", "Electronics", 999, 18))); // 8+10
                assertEquals(1, rows.get(List.of("Monitor", "Electronics", 349, 2)));
            }

            // Step 3: add new premium product with existing orders
            engine.pushChanges("products", ZSet.fromData(productsSchema, allocator, new Object[][]{
                    {"Pen", "Office", 150}, // Pen now premium (price overridden to 150)
            })).step();

            // Pen already has total_qty=100 from step 1 orders — now it joins into report
            try (ZSet repSnap3 = engine.getSnapshot("premium_report")) {
                repSnap3.compact();
                Map<List<Object>, Integer> rows = toMap(repSnap3);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Laptop", "Electronics", 999, 18)));
                assertEquals(1, rows.get(List.of("Monitor", "Electronics", 349, 2)));
                assertEquals(1, rows.get(List.of("Pen", "Office", 150, 100)));
            }
        }
    }
}
