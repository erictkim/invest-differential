package com.invest.differential;

import com.invest.differential.io.AccumulatorSinkOperator;
import com.invest.differential.io.BitemporalAccumulator;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BitemporalAccumulatorTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() { allocator = new RootAllocator(); }

    @AfterEach
    void tearDown() { allocator.close(); }

    @Test
    void accumulatesViewWithStartAndEndTimes() {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        AtomicLong tick = new AtomicLong(1000L);
        AccumulatorSinkOperator sink;

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                  .sql("SELECT customer, amount FROM orders WHERE amount > 100", "big_orders");

            // Manually attach sink with deterministic clock so we can assert exact times
            com.invest.differential.operator.Stream stream =
                    engine.getCircuit().getOutputs().get(0).getOutput();
            Schema viewSchema = new Schema(List.of(
                    Field.notNullable("customer", new ArrowType.Utf8()),
                    Field.notNullable("amount", new ArrowType.Int(32, true))));
            sink = new AccumulatorSinkOperator("big_orders", stream, viewSchema, tick::getAndIncrement);
            engine.getCircuit().addOperator(sink);

            // step 1: insert 3 rows (2 qualify) — clock=1000
            engine.pushChanges("orders", ZSet.fromData(ordersSchema, allocator, new Object[][]{
                    {1, "Alice", 999},
                    {2, "Bob", 50},
                    {3, "Charlie", 349}
            })).step();

            // step 2: delete Charlie's qualifying row — clock=1001
            ZSet del;
            try (ZSet src = ZSet.fromData(ordersSchema, allocator,
                    new Object[][]{{3, "Charlie", 349}})) {
                del = src.negate();
            }
            engine.pushChanges("orders", del).step();
        } // engine.close() → AccumulatorSinkOperator.close() flushes still-live rows (clock=1002)

        // Schema carries field names + types we passed in
        assertEquals(2, sink.schema().getFields().size());
        assertEquals("customer", sink.schema().getFields().get(0).getName());
        assertEquals("amount", sink.schema().getFields().get(1).getName());

        List<BitemporalAccumulator> rows = sink.closedRows();
        assertEquals(2, rows.size());

        Map<String, BitemporalAccumulator> byCustomer = new HashMap<>();
        for (BitemporalAccumulator r : rows) {
            byCustomer.put(asString(r.values()[0]), r);
        }

        BitemporalAccumulator charlie = byCustomer.get("Charlie");
        assertNotNull(charlie);
        assertEquals(349, charlie.values()[1]);
        assertEquals(1000L, charlie.startTime());
        assertEquals(1001L, charlie.endTime());

        BitemporalAccumulator alice = byCustomer.get("Alice");
        assertNotNull(alice);
        assertEquals(999, alice.values()[1]);
        assertEquals(1000L, alice.startTime());
        assertEquals(1002L, alice.endTime());
    }

    @Test
    void engineApi_accumulateView() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        AccumulatorSinkOperator sink;
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name FROM users", "all_users");
            sink = engine.accumulateView("all_users");

            engine.pushChanges("users", ZSet.fromData(schema, allocator,
                    new Object[][]{{1, "alice"}, {2, "bob"}}), 100L).step();

            ZSet del;
            try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{1, "alice"}})) {
                del = src.negate();
            }
            engine.pushChanges("users", del, 200L).step();

            engine.setEventTime(300L); // close-time stamp for still-live "bob"
        }

        // Schema reflects user-facing view column names (Calcite uppercases SELECT names)
        Schema viewSchema = sink.schema();
        assertTrue(viewSchema.getFields().stream().anyMatch(f -> f.getName().equals("ID")));
        assertTrue(viewSchema.getFields().stream().anyMatch(f -> f.getName().equals("NAME")));

        List<BitemporalAccumulator> rows = sink.closedRows();
        assertEquals(2, rows.size());

        Map<String, BitemporalAccumulator> byName = new HashMap<>();
        for (BitemporalAccumulator r : rows) byName.put(asString(r.values()[1]), r);

        BitemporalAccumulator alice = byName.get("alice");
        assertNotNull(alice);
        assertEquals(1, alice.values()[0]);
        assertEquals(100L, alice.startTime());
        assertEquals(200L, alice.endTime());

        BitemporalAccumulator bob = byName.get("bob");
        assertNotNull(bob);
        assertEquals(2, bob.values()[0]);
        assertEquals(100L, bob.startTime());
        assertEquals(300L, bob.endTime());
    }

    @Test
    void engineApi_accumulateTable() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        AccumulatorSinkOperator sink;
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name FROM users", "echo");
            sink = engine.accumulateTable("users");

            engine.pushChanges("users", ZSet.fromData(schema, allocator,
                    new Object[][]{{1, "alice"}, {2, "bob"}}), 50L).step();
            engine.setEventTime(75L);
        }

        List<BitemporalAccumulator> rows = sink.closedRows();
        assertEquals(2, rows.size());
        for (BitemporalAccumulator r : rows) {
            assertEquals(50L, r.startTime());
            assertEquals(75L, r.endTime());
        }
        // Original input table schema preserved
        assertEquals("id", sink.schema().getFields().get(0).getName());
        assertEquals("name", sink.schema().getFields().get(1).getName());
    }

    @Test
    void liveRowsVisibleViaRowsBeforeClose() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true))));

        AtomicLong tick = new AtomicLong(10L);
        // Construct sink standalone (no Stream attachment needed for direct recordDelta use)
        com.invest.differential.operator.Stream stream =
                new com.invest.differential.operator.Stream(schema);
        AccumulatorSinkOperator sink = new AccumulatorSinkOperator(
                "t", stream, schema, tick::getAndIncrement);

        try (ZSet z = ZSet.fromData(schema, allocator, new Object[][]{{1}, {2}})) {
            sink.recordDelta(z); // clock=10
        }
        // closedRows is empty (nothing retracted)
        assertEquals(0, sink.closedRows().size());
        // rows() snapshot includes the live rows with end=current clock
        List<BitemporalAccumulator> snapshot = sink.rows();
        assertEquals(2, snapshot.size());
        for (BitemporalAccumulator r : snapshot) {
            assertEquals(10L, r.startTime());
            assertEquals(11L, r.endTime()); // clock incremented by snapshot read
        }

        // After close, the still-live rows are flushed into closedRows
        sink.close();
        assertEquals(2, sink.closedRows().size());
        for (BitemporalAccumulator r : sink.closedRows()) {
            assertEquals(10L, r.startTime());
            assertEquals(12L, r.endTime());
        }

        // Subsequent recordDelta is rejected
        assertThrows(IllegalStateException.class, () -> sink.recordDelta(null));
    }

    @Test
    void multipleInsertsOfSameRowTrackedSeparately() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true))));

        AtomicLong tick = new AtomicLong(100L);
        com.invest.differential.operator.Stream stream =
                new com.invest.differential.operator.Stream(schema);
        AccumulatorSinkOperator sink = new AccumulatorSinkOperator(
                "t", stream, schema, tick::getAndIncrement);

        // Insert {1} twice at clock=100
        try (ZSet z = ZSet.fromData(schema, allocator,
                new Object[][]{{1}, {1}})) {
            sink.recordDelta(z);
        }
        // Retract {1} once at clock=101
        ZSet del;
        try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{1}})) {
            del = src.negate();
        }
        try (ZSet d = del) {
            sink.recordDelta(d);
        }

        // One closed row (start=100, end=101), one still live
        assertEquals(1, sink.closedRows().size());
        BitemporalAccumulator closed = sink.closedRows().get(0);
        assertEquals(100L, closed.startTime());
        assertEquals(101L, closed.endTime());

        sink.close(); // clock=102 flushes the second
        assertEquals(2, sink.closedRows().size());
        BitemporalAccumulator flushed = sink.closedRows().get(1);
        assertEquals(100L, flushed.startTime());
        assertEquals(102L, flushed.endTime());
    }

    private static String asString(Object v) {
        if (v instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return v == null ? null : v.toString();
    }
}
