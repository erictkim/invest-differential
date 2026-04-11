package com.invest;

import com.invest.differential.IncrementalEngine;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        System.out.println("=== Incremental View Maintenance Engine Demo ===\n");

        try (BufferAllocator allocator = new RootAllocator()) {
            demoFilterAndProject(allocator);
            demoAggregation(allocator);
            demoJoin(allocator);
        }
    }

    /**
     * Demo 1: A simple filter+project query with incremental updates.
     * Simulates an order stream where we track high-value orders (amount > 100).
     */
    private static void demoFilterAndProject(BufferAllocator allocator) {
        System.out.println("--- Demo 1: Filter + Project (High-Value Orders) ---\n");

        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                  .sql("SELECT customer, product, amount FROM orders WHERE amount > 100");

            // --- Step 1: Initial batch of orders ---
            System.out.println("Step 1: Insert 4 orders");
            ZSet batch1 = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, "Alice",   "Laptop",     999},
                    {2, "Bob",     "Mouse",       25},
                    {3, "Charlie", "Monitor",    349},
                    {4, "Alice",   "Keyboard",    75},
            });
            engine.pushChanges("orders", batch1).step();
            try (ZSet delta1 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta1);
                System.out.println("  (Laptop $999 and Monitor $349 pass the filter; Mouse $25 and Keyboard $75 do not)\n");
            }

            // --- Step 2: New orders arrive ---
            System.out.println("Step 2: Insert 2 more orders");
            ZSet batch2 = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {5, "Bob",     "Headphones", 199},
                    {6, "Charlie", "Webcam",      45},
            });
            engine.pushChanges("orders", batch2).step();
            try (ZSet delta2 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta2);
                System.out.println("  (Only Headphones $199 is new in the view; Webcam $45 filtered out)\n");
            }

            // --- Step 3: Delete an order (negative weight) ---
            System.out.println("Step 3: Delete Alice's Laptop order and insert a replacement");
            ZSet combined;
            try (ZSet delSrc = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
            })) {
                try (ZSet deletion = delSrc.negate();
                     ZSet insertion = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                             {7, "Alice", "Laptop Pro", 1299},
                     })) {
                    combined = deletion.add(insertion);
                }
            }
            engine.pushChanges("orders", combined).step();
            try (ZSet delta3 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta3);
                System.out.println("  (Laptop removed with weight -1, Laptop Pro added with weight +1)\n");
            }

            // --- Step 4: No changes ---
            System.out.println("Step 4: No changes pushed (empty delta)");
            engine.pushChanges("orders", ZSet.empty(ordersSchema, allocator)).step();
            try (ZSet delta4 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta4);
                System.out.println("  (Empty — the view is stable)\n");
            }
        }
    }

    /**
     * Demo 2: Aggregation with GROUP BY — revenue per product category.
     * Shows how aggregates update incrementally as data changes.
     */
    private static void demoAggregation(BufferAllocator allocator) {
        System.out.println("--- Demo 2: Aggregation (Revenue Per Product) ---\n");

        Schema salesSchema = new Schema(List.of(
                Field.notNullable("sale_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", salesSchema)
                  .sql("SELECT product, SUM(revenue) as total_revenue, COUNT(*) as num_sales " +
                       "FROM sales GROUP BY product");

            // --- Step 1: Initial sales data ---
            System.out.println("Step 1: Insert initial sales");
            ZSet batch1 = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {1, "Widget",  50},
                    {2, "Gadget", 120},
                    {3, "Widget",  75},
                    {4, "Gadget",  80},
                    {5, "Gizmo",  200},
            });
            engine.pushChanges("sales", batch1).step();
            try (ZSet delta1 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta1);
                System.out.println("  (Widget: $125 / 2 sales, Gadget: $200 / 2 sales, Gizmo: $200 / 1 sale)\n");
            }

            // --- Step 2: More Widget sales ---
            System.out.println("Step 2: Insert 2 more Widget sales");
            ZSet batch2 = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {6, "Widget", 60},
                    {7, "Widget", 90},
            });
            engine.pushChanges("sales", batch2).step();
            try (ZSet delta2 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta2);
                System.out.println("  (Old Widget aggregate removed [-1], new Widget aggregate added [+1])");
                System.out.println("  (Gadget and Gizmo unchanged — not in delta)\n");
            }

            // --- Step 3: Correct a sale (delete + re-insert with new value) ---
            System.out.println("Step 3: Correct Gizmo sale #5: revenue $200 → $250");
            ZSet correction;
            try (ZSet delSrc = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {5, "Gizmo", 200},
            })) {
                try (ZSet del = delSrc.negate();
                     ZSet ins = ZSet.fromData(salesSchema, allocator, new Object[][]{
                             {5, "Gizmo", 250},
                     })) {
                    correction = del.add(ins);
                }
            }
            engine.pushChanges("sales", correction).step();
            try (ZSet delta3 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta3);
                System.out.println("  (Gizmo: old total $200 removed, new total $250 added)\n");
            }

            // --- Step 4: New product category appears ---
            System.out.println("Step 4: New product 'Doohickey' enters the market");
            ZSet batch4 = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {8,  "Doohickey", 30},
                    {9,  "Doohickey", 45},
                    {10, "Gadget",   150},
            });
            engine.pushChanges("sales", batch4).step();
            try (ZSet delta4 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta4);
                System.out.println("  (Doohickey appears as a new group; Gadget aggregate updated)\n");
            }
        }

    }

    /**
     * Demo 3: Join — enrich orders with customer details.
     * Shows how changes on either side of the join propagate incrementally.
     */
    private static void demoJoin(BufferAllocator allocator) {
        System.out.println("--- Demo 3: Join (Orders + Customers) ---\n");

        Schema customersSchema = new Schema(List.of(
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8()),
                Field.notNullable("tier", new ArrowType.Utf8())
        ));

        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("cust_id", new ArrowType.Int(32, true)),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("customers", customersSchema)
                  .registerTable("orders", ordersSchema)
                  .sql("SELECT c.name, c.tier, o.product, o.amount " +
                       "FROM orders o JOIN customers c ON o.cust_id = c.cust_id");

            // --- Step 1: Load customers, then orders ---
            System.out.println("Step 1: Insert 3 customers and 3 orders");
            engine.pushChanges("customers", ZSet.fromData(customersSchema, allocator, new Object[][]{
                    {1, "Alice",   "Gold"},
                    {2, "Bob",     "Silver"},
                    {3, "Charlie", "Bronze"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {101, 1, "Laptop",  999},
                    {102, 2, "Mouse",    25},
                    {103, 1, "Monitor", 349},
            }));
            engine.step();
            try (ZSet delta1 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta1);
                System.out.println("  (3 joined rows: Alice's 2 orders + Bob's 1 order; Charlie has no orders)\n");
            }

            // --- Step 2: New order for existing customer ---
            System.out.println("Step 2: Bob places a new order");
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {104, 2, "Keyboard", 75},
            })).step();
            try (ZSet delta2 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta2);
                System.out.println("  (Bob's new order joins with his existing customer record)\n");
            }

            // --- Step 3: Update customer tier (delete + re-insert) ---
            System.out.println("Step 3: Upgrade Alice from Gold to Platinum");
            ZSet tierChange;
            try (ZSet oldAlice = ZSet.fromData(customersSchema, allocator, new Object[][]{
                    {1, "Alice", "Gold"},
            })) {
                try (ZSet neg = oldAlice.negate();
                     ZSet newAlice = ZSet.fromData(customersSchema, allocator, new Object[][]{
                             {1, "Alice", "Platinum"},
                     })) {
                    tierChange = neg.add(newAlice);
                }
            }
            engine.pushChanges("customers", tierChange).step();
            try (ZSet delta3 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta3);
                System.out.println("  (Alice's 2 orders re-join: old Gold rows retracted [-1], new Platinum rows added [+1])\n");
            }

            // --- Step 4: New customer with immediate order ---
            System.out.println("Step 4: New customer Diana places an order");
            engine.pushChanges("customers", ZSet.fromData(customersSchema, allocator, new Object[][]{
                    {4, "Diana", "Gold"},
            }));
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {105, 4, "Tablet", 499},
            }));
            engine.step();
            try (ZSet delta4 = engine.getOutput()) {
                System.out.println("  Output delta: " + delta4);
                System.out.println("  (Diana's order joins with her new customer record)\n");
            }
        }

        System.out.println("=== Demo Complete ===");
    }
}