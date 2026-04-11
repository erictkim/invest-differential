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
            ZSet delta1 = engine.getOutput();
            System.out.println("  Output delta: " + delta1);
            System.out.println("  (Laptop $999 and Monitor $349 pass the filter; Mouse $25 and Keyboard $75 do not)\n");
            delta1.close();
            batch1.close();

            // --- Step 2: New orders arrive ---
            System.out.println("Step 2: Insert 2 more orders");
            ZSet batch2 = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {5, "Bob",     "Headphones", 199},
                    {6, "Charlie", "Webcam",      45},
            });
            engine.pushChanges("orders", batch2).step();
            ZSet delta2 = engine.getOutput();
            System.out.println("  Output delta: " + delta2);
            System.out.println("  (Only Headphones $199 is new in the view; Webcam $45 filtered out)\n");
            delta2.close();
            batch2.close();

            // --- Step 3: Delete an order (negative weight) ---
            System.out.println("Step 3: Delete Alice's Laptop order and insert a replacement");
            ZSet delSrc = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, "Alice", "Laptop", 999},
            });
            ZSet deletion = delSrc.negate();
            delSrc.close();
            ZSet insertion = ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {7, "Alice", "Laptop Pro", 1299},
            });
            ZSet combined = deletion.add(insertion);

            engine.pushChanges("orders", combined).step();
            ZSet delta3 = engine.getOutput();
            System.out.println("  Output delta: " + delta3);
            System.out.println("  (Laptop removed with weight -1, Laptop Pro added with weight +1)\n");
            delta3.close();
            combined.close();
            deletion.close();
            insertion.close();

            // --- Step 4: No changes ---
            System.out.println("Step 4: No changes pushed (empty delta)");
            ZSet empty = ZSet.empty(ordersSchema, allocator);
            engine.pushChanges("orders", empty).step();
            ZSet delta4 = engine.getOutput();
            System.out.println("  Output delta: " + delta4);
            System.out.println("  (Empty — the view is stable)\n");
            delta4.close();
            empty.close();
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
            ZSet delta1 = engine.getOutput();
            System.out.println("  Output delta: " + delta1);
            System.out.println("  (Widget: $125 / 2 sales, Gadget: $200 / 2 sales, Gizmo: $200 / 1 sale)\n");
            delta1.close();
            batch1.close();

            // --- Step 2: More Widget sales ---
            System.out.println("Step 2: Insert 2 more Widget sales");
            ZSet batch2 = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {6, "Widget", 60},
                    {7, "Widget", 90},
            });
            engine.pushChanges("sales", batch2).step();
            ZSet delta2 = engine.getOutput();
            System.out.println("  Output delta: " + delta2);
            System.out.println("  (Old Widget aggregate removed [-1], new Widget aggregate added [+1])");
            System.out.println("  (Gadget and Gizmo unchanged — not in delta)\n");
            delta2.close();
            batch2.close();

            // --- Step 3: Correct a sale (delete + re-insert with new value) ---
            System.out.println("Step 3: Correct Gizmo sale #5: revenue $200 → $250");
            ZSet delSrc = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {5, "Gizmo", 200},
            });
            ZSet del = delSrc.negate();
            delSrc.close();
            ZSet ins = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {5, "Gizmo", 250},
            });
            ZSet correction = del.add(ins);
            engine.pushChanges("sales", correction).step();
            ZSet delta3 = engine.getOutput();
            System.out.println("  Output delta: " + delta3);
            System.out.println("  (Gizmo: old total $200 removed, new total $250 added)\n");
            delta3.close();
            correction.close();
            del.close();
            ins.close();

            // --- Step 4: New product category appears ---
            System.out.println("Step 4: New product 'Doohickey' enters the market");
            ZSet batch4 = ZSet.fromData(salesSchema, allocator, new Object[][]{
                    {8,  "Doohickey", 30},
                    {9,  "Doohickey", 45},
                    {10, "Gadget",   150},
            });
            engine.pushChanges("sales", batch4).step();
            ZSet delta4 = engine.getOutput();
            System.out.println("  Output delta: " + delta4);
            System.out.println("  (Doohickey appears as a new group; Gadget aggregate updated)\n");
            delta4.close();
            batch4.close();
        }

        System.out.println("=== Demo Complete ===");
    }
}