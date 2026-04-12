package com.invest.differential;

import com.invest.differential.operator.Circuit;
import com.invest.differential.operator.OperatorMetrics;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OperatorMetricsTest {

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
    void metricsTrackStepCount() {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT val FROM t WHERE val > 10")
                    .setMetricsEnabled(true);

            // Step 1
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{5}, {15}, {25}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            // Step 2
            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{30}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            assertFalse(metrics.isEmpty(), "Should have metrics entries");

            // Every operator should have step count = 2
            for (var entry : metrics.entrySet()) {
                assertEquals(2, entry.getValue().getStepCount(),
                        "Operator " + entry.getKey() + " should have 2 steps");
            }
        }
    }

    @Test
    void metricsTrackRowsProduced() {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT val FROM t")
                    .setMetricsEnabled(true);

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{1}, {2}, {3}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            // The output operator should have produced rows
            long totalRows = 0;
            for (var entry : metrics.entrySet()) {
                if (entry.getKey().contains("Output")) {
                    totalRows += entry.getValue().getRowsProduced();
                }
            }
            assertTrue(totalRows > 0, "Output operator should have produced rows");
        }
    }

    @Test
    void metricsTrackTime() {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT val FROM t")
                    .setMetricsEnabled(true);

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{1}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            for (var entry : metrics.entrySet()) {
                assertTrue(entry.getValue().getTotalNanos() >= 0,
                        "Total nanos should be non-negative for " + entry.getKey());
                assertTrue(entry.getValue().getTotalMillis() >= 0,
                        "Total millis should be non-negative for " + entry.getKey());
            }
        }
    }

    @Test
    void metricsDisabledByDefault() {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT val FROM t");

            assertFalse(engine.getCircuit().isMetricsEnabled());

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{1}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            // Metrics objects exist but should not have been updated
            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            for (var entry : metrics.entrySet()) {
                assertEquals(0, entry.getValue().getStepCount(),
                        "Steps should be 0 when metrics disabled for " + entry.getKey());
            }
        }
    }

    @Test
    void metricsToStringFormat() {
        OperatorMetrics m = new OperatorMetrics();
        m.recordStep(1_000_000, 5); // 1ms, 5 rows
        m.recordStep(2_000_000, 3); // 2ms, 3 rows

        String s = m.toString();
        assertTrue(s.contains("steps=2"));
        assertTrue(s.contains("rows=8"));
        assertEquals(3.0, m.getTotalMillis(), 0.5);
        assertEquals(1.5, m.getAvgStepMillis(), 0.5);
    }

    @Test
    void metricsReset() {
        OperatorMetrics m = new OperatorMetrics();
        m.recordStep(1_000_000, 10);
        assertEquals(1, m.getStepCount());
        assertEquals(10, m.getRowsProduced());

        m.reset();
        assertEquals(0, m.getStepCount());
        assertEquals(0, m.getRowsProduced());
        assertEquals(0, m.getTotalNanos());
    }

    @Test
    void metricsWithJoinCountsCorrectly() {
        Schema left = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        Schema right = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", left)
                    .registerTable("scores", right)
                    .sql("SELECT u.name, s.score FROM users u INNER JOIN scores s ON u.id = s.id")
                    .setMetricsEnabled(true);

            engine.pushChanges("users", ZSet.fromData(left, allocator, new Object[][]{
                    {1, "Alice"}, {2, "Bob"}
            }));
            engine.pushChanges("scores", ZSet.fromData(right, allocator, new Object[][]{
                    {1, 95}, {2, 80}
            }));
            engine.step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            // Should have multiple operators, each with 1 step
            assertTrue(metrics.size() >= 3, "Join query should produce at least 3 operators");
            for (var entry : metrics.entrySet()) {
                assertEquals(1, entry.getValue().getStepCount());
            }
        }
    }

    @Test
    void metricsAccumulateAcrossSteps() {
        Schema schema = new Schema(List.of(
                new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("emp", schema)
                    .sql("SELECT dept, SUM(salary) FROM emp GROUP BY dept")
                    .setMetricsEnabled(true);

            // 3 steps
            for (int i = 0; i < 3; i++) {
                engine.pushChanges("emp", ZSet.fromData(schema, allocator, new Object[][]{
                        {"eng", 100 + i}
                })).step();
                try (ZSet r = engine.getOutput()) { r.compact(); }
            }

            Map<String, OperatorMetrics> metrics = engine.getCircuit().getMetrics();
            for (var entry : metrics.entrySet()) {
                assertEquals(3, entry.getValue().getStepCount(),
                        "Each operator should have 3 steps: " + entry.getKey());
                assertTrue(entry.getValue().getTotalNanos() > 0,
                        "Total nanos should be > 0 after 3 steps: " + entry.getKey());
            }
        }
    }

    @Test
    void getMetricsByIndex() {
        Schema schema = new Schema(List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT val FROM t")
                    .setMetricsEnabled(true);

            engine.pushChanges("t", ZSet.fromData(schema, allocator, new Object[][]{{1}})).step();
            try (ZSet r = engine.getOutput()) { r.compact(); }

            Circuit circuit = engine.getCircuit();
            // First operator (Input) should have metrics
            OperatorMetrics m = circuit.getMetrics(0);
            assertNotNull(m);
            assertEquals(1, m.getStepCount());
        }
    }
}
