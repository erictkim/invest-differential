package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the engine-aware built-in SQL functions {@code EVENT_TIME()} and
 * {@code ROW_CREATED_AT()}.
 */
class EventTimeFunctionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() { allocator = new RootAllocator(); }

    @AfterEach
    void tearDown() {
        try { allocator.close(); } catch (IllegalStateException e) { /* leak detection */ }
    }

    private static Map<List<Object>, Integer> toMap(ZSet z) {
        Map<List<Object>, Integer> m = new LinkedHashMap<>();
        for (int i = 0; i < z.rowCount(); i++) {
            m.merge(List.of(z.getDataValues(i)), z.getWeight(i), Integer::sum);
        }
        return m;
    }

    @Test
    void eventTimeFunction_reflectsCurrentEventTime() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", schema)
                  .sql("SELECT id, name, EVENT_TIME() AS et FROM users");

            engine.pushChanges("users",
                    ZSet.fromData(schema, allocator, new Object[][]{{1, "alice"}}),
                    1000L).step();

            try (ZSet out = engine.getOutput()) {
                out.compact();
                Map<List<Object>, Integer> rows = toMap(out);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(1, "alice", 1000L)));
            }

            engine.pushChanges("users",
                    ZSet.fromData(schema, allocator, new Object[][]{{2, "bob"}}),
                    2000L).step();

            try (ZSet out = engine.getOutput()) {
                out.compact();
                Map<List<Object>, Integer> rows = toMap(out);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(2, "bob", 2000L)));
            }
        }
    }

    @Test
    void capturedCreationTime_viaProjectionOverInput() {
        // EVENT_TIME() returns the *current* clock at evaluation time. To
        // capture per-row creation time, project it once over the input table
        // so each row carries its own insertion timestamp as a plain column.
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                  .sql("SELECT id, EVENT_TIME() AS created_at FROM t", "t_stamped")
                  .sql("SELECT id, created_at FROM t_stamped", "downstream");

            engine.pushChanges("t",
                    ZSet.fromData(schema, allocator, new Object[][]{{1}, {2}}),
                    100L).step();

            // Push more rows at a different event_time. The earlier rows must
            // keep their original created_at (100), not be re-stamped to 200.
            engine.pushChanges("t",
                    ZSet.fromData(schema, allocator, new Object[][]{{3}}),
                    200L).step();

            try (ZSet snap = engine.getSnapshot("downstream")) {
                snap.compact();
                Map<List<Object>, Integer> rows = toMap(snap);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of(1, 100L)));
                assertEquals(1, rows.get(List.of(2, 100L)));
                assertEquals(1, rows.get(List.of(3, 200L)));
            }
        }
    }

    @Test
    void eventTimeInFilter() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true))
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                  .sql("SELECT id FROM t WHERE EVENT_TIME() >= 1000");

            engine.setEventTime(500L);
            engine.pushChanges("t",
                    ZSet.fromData(schema, allocator, new Object[][]{{1}})).step();

            try (ZSet out = engine.getOutput()) {
                out.compact();
                assertEquals(0, out.rowCount(), "row pushed at event_time=500 should be filtered out");
            }

            engine.pushChanges("t",
                    ZSet.fromData(schema, allocator, new Object[][]{{2}}),
                    1500L).step();

            try (ZSet out = engine.getOutput()) {
                out.compact();
                Map<List<Object>, Integer> rows = toMap(out);
                assertEquals(1, rows.size());
                assertEquals(1, rows.get(List.of(2)));
            }
        }
    }

    @Test
    void eventTimeFallsBackToWallClock_whenNeverSet() {
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true))
        ));

        long before = System.currentTimeMillis();

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                  .sql("SELECT id, EVENT_TIME() AS et FROM t");

            engine.pushChanges("t",
                    ZSet.fromData(schema, allocator, new Object[][]{{1}})).step();

            try (ZSet out = engine.getOutput()) {
                out.compact();
                assertEquals(1, out.rowCount());
                long et = (Long) out.getDataValues(0)[1];
                long after = System.currentTimeMillis();
                assertTrue(et >= before && et <= after,
                        "EVENT_TIME() should fall back to wall clock; got " + et
                                + " (window " + before + "-" + after + ")");
            }
        }
    }
}
