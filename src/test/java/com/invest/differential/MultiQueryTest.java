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
}
