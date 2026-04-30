package com.invest.differential;

import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ParquetReader;
import com.invest.differential.io.ParquetSerializer;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetSerializerTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() { allocator = new RootAllocator(); }

    @AfterEach
    void tearDown() { allocator.close(); }

    @Test
    void writesViewWithStartAndEndTimes(@TempDir Path tmp) throws Exception {
        Schema ordersSchema = new Schema(List.of(
                Field.notNullable("order_id", new ArrowType.Int(32, true)),
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));

        File outFile = tmp.resolve("orders_view.parquet").toFile();
        AtomicLong tick = new AtomicLong(1000L);

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("orders", ordersSchema)
                  .sql("SELECT customer, amount FROM orders WHERE amount > 100", "big_orders");

            // Manually attach serializer with deterministic clock so we can assert exact times
            com.invest.differential.operator.Stream stream =
                    engine.getCircuit().getOutputs().get(0).getOutput();
            // Build a view schema with the names the user expects
            Schema viewSchema = new Schema(List.of(
                    Field.notNullable("customer", new ArrowType.Utf8()),
                    Field.notNullable("amount", new ArrowType.Int(32, true))));
            ParquetSerializer ser = ParquetSerializer.create(viewSchema, outFile,
                    () -> tick.getAndIncrement());
            engine.getCircuit().addOperator(
                    new com.invest.differential.io.ParquetSinkOperator("big_orders", stream, ser));

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
        } // engine.close() flushes Alice (still live)

        assertTrue(outFile.exists() && outFile.length() > 0);

        List<Map<String, Object>> rows;
        try (Stream<Map<String, Object>> s = ParquetReader.streamContent(outFile, mapHydrator())) {
            rows = s.toList();
        }

        // Expect: Charlie record (start=1000, end=1001) + Alice flushed at close (start=1000, end=1002)
        assertEquals(2, rows.size());
        Map<String, Map<String, Object>> byCustomer = new HashMap<>();
        for (Map<String, Object> r : rows) {
            byCustomer.put((String) r.get("customer"), r);
        }
        Map<String, Object> charlie = byCustomer.get("Charlie");
        assertNotNull(charlie);
        assertEquals(349, charlie.get("amount"));
        assertEquals(1000L, charlie.get("start_time"));
        assertEquals(1001L, charlie.get("end_time"));

        Map<String, Object> alice = byCustomer.get("Alice");
        assertNotNull(alice);
        assertEquals(999, alice.get("amount"));
        assertEquals(1000L, alice.get("start_time"));
        assertEquals(1002L, alice.get("end_time"));
    }

    @Test
    void writesTableSnapshot(@TempDir Path tmp) throws Exception {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        File outFile = tmp.resolve("users.parquet").toFile();
        AtomicLong tick = new AtomicLong(1L);

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name FROM users", "echo");

            com.invest.differential.operator.InputOperator inp =
                    engine.getCircuit().getInput("users");
            ParquetSerializer ser = ParquetSerializer.create(schema, outFile,
                    () -> tick.getAndIncrement());
            engine.getCircuit().addOperator(
                    new com.invest.differential.io.ParquetSinkOperator("users", inp.getOutput(), ser));

            engine.pushChanges("users", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, "alice"}, {2, "bob"}
            })).step();
        }

        try (Stream<Map<String, Object>> s = ParquetReader.streamContent(outFile, mapHydrator())) {
            List<Map<String, Object>> rows = s.toList();
            assertEquals(2, rows.size());
        }
    }

    @Test
    void engineApi_writeViewToParquet(@TempDir Path tmp) throws Exception {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));
        File outFile = tmp.resolve("named_view.parquet").toFile();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name FROM users", "all_users")
                  .writeViewToParquet("all_users", outFile);

            engine.pushChanges("users", ZSet.fromData(schema, allocator,
                    new Object[][]{{1, "alice"}, {2, "bob"}})).step();
        }

        try (Stream<Map<String, Object>> s = ParquetReader.streamContent(outFile, mapHydrator())) {
            List<Map<String, Object>> rows = s.toList();
            assertEquals(2, rows.size());
            // Calcite uppercases column names from SELECT id, name
            assertTrue(rows.get(0).containsKey("NAME"));
            assertTrue(rows.get(0).containsKey("ID"));
            assertTrue(rows.get(0).containsKey("start_time"));
            assertTrue(rows.get(0).containsKey("end_time"));
        }
    }

    private static HydratorSupplier<Map<String, Object>, Map<String, Object>> mapHydrator() {
        return columns -> new Hydrator<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> start() { return new LinkedHashMap<>(); }

            @Override
            public Map<String, Object> add(Map<String, Object> target, String heading, Object value) {
                target.put(heading, value);
                return target;
            }

            @Override
            public Map<String, Object> finish(Map<String, Object> target) { return target; }
        };
    }
}
