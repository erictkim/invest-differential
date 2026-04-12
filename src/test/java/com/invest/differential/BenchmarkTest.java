package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmarking suite for key operators and end-to-end incremental queries.
 * Uses warmup iterations and multiple measured runs to produce stable timings.
 * Tagged as "benchmark" so they can be included/excluded via surefire config.
 */
@Tag("benchmark")
class BenchmarkTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURED_ITERATIONS = 5;
    private static final int ROWS = 10_000;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    // ── Data generation helpers ──

    private static final Schema INT_SCHEMA = new Schema(List.of(
            new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));

    private static final Schema TWO_INT_SCHEMA = new Schema(List.of(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));

    private static final Schema STRING_INT_SCHEMA = new Schema(List.of(
            new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));

    private static final Schema JOIN_LEFT = new Schema(List.of(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private static final Schema JOIN_RIGHT = new Schema(List.of(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));

    private Object[][] generateIntData(int rows) {
        Object[][] data = new Object[rows][1];
        for (int i = 0; i < rows; i++) data[i] = new Object[]{i};
        return data;
    }

    private Object[][] generateTwoIntData(int rows) {
        Object[][] data = new Object[rows][2];
        for (int i = 0; i < rows; i++) data[i] = new Object[]{i % 100, i * 10};
        return data;
    }

    private Object[][] generateDeptSalaryData(int rows) {
        String[] depts = {"eng", "sales", "hr", "marketing", "ops"};
        Object[][] data = new Object[rows][2];
        for (int i = 0; i < rows; i++) data[i] = new Object[]{depts[i % depts.length], 50000 + (i * 100)};
        return data;
    }

    private Object[][] generateJoinLeftData(int rows) {
        Object[][] data = new Object[rows][2];
        for (int i = 0; i < rows; i++) data[i] = new Object[]{i, "user_" + i};
        return data;
    }

    private Object[][] generateJoinRightData(int rows) {
        Object[][] data = new Object[rows][2];
        for (int i = 0; i < rows; i++) data[i] = new Object[]{i, 50 + (i % 50)};
        return data;
    }

    /**
     * Runs a benchmark: warmup iterations followed by measured iterations.
     * Returns timing stats as a formatted string.
     */
    private BenchmarkResult runBenchmark(String name, Runnable setup, Runnable benchmark) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            if (setup != null) setup.run();
            benchmark.run();
        }

        // Measured
        long[] nanos = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            if (setup != null) setup.run();
            long start = System.nanoTime();
            benchmark.run();
            nanos[i] = System.nanoTime() - start;
        }

        Arrays.sort(nanos);
        BenchmarkResult result = new BenchmarkResult(name, nanos);
        System.out.println(result);
        return result;
    }

    record BenchmarkResult(String name, long[] nanos) {
        double avgMs() {
            long sum = 0;
            for (long n : nanos) sum += n;
            return (sum / (double) nanos.length) / 1_000_000.0;
        }

        double minMs() { return nanos[0] / 1_000_000.0; }
        double maxMs() { return nanos[nanos.length - 1] / 1_000_000.0; }
        double p50Ms() { return nanos[nanos.length / 2] / 1_000_000.0; }
        double p95Ms() { return nanos[(int) (nanos.length * 0.95)] / 1_000_000.0; }

        @Override
        public String toString() {
            return String.format("[BENCHMARK] %-40s avg=%.2fms  min=%.2fms  max=%.2fms  p50=%.2fms  p95=%.2fms  (n=%d)",
                    name, avgMs(), minMs(), maxMs(), p50Ms(), p95Ms(), nanos.length);
        }
    }

    // ── Micro-benchmarks: Filter ──

    @Test
    void benchFilterSelectivity50Pct() {
        Object[][] data = generateIntData(ROWS);
        BenchmarkResult result = runBenchmark("filter_50pct_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", INT_SCHEMA)
                        .sql("SELECT val FROM t WHERE val > " + (ROWS / 2));
                engine.pushChanges("t", ZSet.fromData(INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertTrue(out.rowCount() > 0);
                }
            }
        });
        assertTrue(result.avgMs() < 30_000, "Filter should complete in reasonable time");
    }

    @Test
    void benchFilterSelectivity10Pct() {
        Object[][] data = generateIntData(ROWS);
        BenchmarkResult result = runBenchmark("filter_10pct_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", INT_SCHEMA)
                        .sql("SELECT val FROM t WHERE val > " + (ROWS * 9 / 10));
                engine.pushChanges("t", ZSet.fromData(INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertTrue(out.rowCount() > 0);
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    // ── Micro-benchmarks: Project ──

    @Test
    void benchProjectArithmetic() {
        Object[][] data = generateTwoIntData(ROWS);
        BenchmarkResult result = runBenchmark("project_arithmetic_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", TWO_INT_SCHEMA)
                        .sql("SELECT id, amount, id + amount AS total, amount * 2 AS doubled FROM t");
                engine.pushChanges("t", ZSet.fromData(TWO_INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertEquals(ROWS, out.rowCount());
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    // ── Micro-benchmarks: Aggregate ──

    @Test
    void benchAggregateFewGroups() {
        Object[][] data = generateDeptSalaryData(ROWS);
        BenchmarkResult result = runBenchmark("aggregate_5_groups_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("emp", STRING_INT_SCHEMA)
                        .sql("SELECT dept, SUM(salary), COUNT(*), AVG(salary) FROM emp GROUP BY dept");
                engine.pushChanges("emp", ZSet.fromData(STRING_INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertEquals(5, out.rowCount());
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    @Test
    void benchAggregateManyGroups() {
        Object[][] data = generateTwoIntData(ROWS);
        BenchmarkResult result = runBenchmark("aggregate_100_groups_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", TWO_INT_SCHEMA)
                        .sql("SELECT id, SUM(amount), COUNT(*) FROM t GROUP BY id");
                engine.pushChanges("t", ZSet.fromData(TWO_INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertEquals(100, out.rowCount());
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    // ── Micro-benchmarks: Join ──

    @Test
    void benchInnerJoin() {
        int joinRows = ROWS / 10; // smaller for join to avoid O(n^2)
        Object[][] leftData = generateJoinLeftData(joinRows);
        Object[][] rightData = generateJoinRightData(joinRows);
        BenchmarkResult result = runBenchmark("inner_join_" + joinRows + "_x_" + joinRows, null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("users", JOIN_LEFT)
                        .registerTable("scores", JOIN_RIGHT)
                        .sql("SELECT u.name, s.score FROM users u INNER JOIN scores s ON u.id = s.id");
                engine.pushChanges("users", ZSet.fromData(JOIN_LEFT, allocator, leftData));
                engine.pushChanges("scores", ZSet.fromData(JOIN_RIGHT, allocator, rightData));
                engine.step();
                try (ZSet out = engine.getOutput()) {
                    assertEquals(joinRows, out.rowCount());
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    @Test
    void benchLeftJoin() {
        int joinRows = ROWS / 10;
        Object[][] leftData = generateJoinLeftData(joinRows);
        // Right side has half the rows
        Object[][] rightData = generateJoinRightData(joinRows / 2);
        BenchmarkResult result = runBenchmark("left_join_" + joinRows + "_x_" + (joinRows / 2), null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("users", JOIN_LEFT)
                        .registerTable("scores", JOIN_RIGHT)
                        .sql("SELECT u.name, s.score FROM users u LEFT JOIN scores s ON u.id = s.id");
                engine.pushChanges("users", ZSet.fromData(JOIN_LEFT, allocator, leftData));
                engine.pushChanges("scores", ZSet.fromData(JOIN_RIGHT, allocator, rightData));
                engine.step();
                try (ZSet out = engine.getOutput()) {
                    assertTrue(out.rowCount() >= joinRows);
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    // ── Macro-benchmarks: Incremental step performance ──

    @Test
    void benchIncrementalSteps() {
        int batchSize = 100;
        int steps = 20;
        Object[][] initialData = generateDeptSalaryData(ROWS);
        BenchmarkResult result = runBenchmark("incremental_" + steps + "_steps_" + batchSize + "_rows_each", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("emp", STRING_INT_SCHEMA)
                        .sql("SELECT dept, SUM(salary), COUNT(*) FROM emp GROUP BY dept");
                // Initial load
                engine.pushChanges("emp", ZSet.fromData(STRING_INT_SCHEMA, allocator, initialData)).step();
                try (ZSet out = engine.getOutput()) { out.compact(); }

                // Incremental steps
                String[] depts = {"eng", "sales", "hr", "marketing", "ops"};
                for (int s = 0; s < steps; s++) {
                    Object[][] delta = new Object[batchSize][2];
                    for (int i = 0; i < batchSize; i++) {
                        delta[i] = new Object[]{depts[i % depts.length], 60000 + s * 100 + i};
                    }
                    engine.pushChanges("emp", ZSet.fromData(STRING_INT_SCHEMA, allocator, delta)).step();
                    try (ZSet out = engine.getOutput()) { out.compact(); }
                }
            }
        });
        assertTrue(result.avgMs() < 60_000);
    }

    @Test
    void benchIncrementalJoinUpdates() {
        int initialRows = 500;
        int deltaSize = 50;
        int steps = 10;
        Object[][] leftData = generateJoinLeftData(initialRows);
        Object[][] rightData = generateJoinRightData(initialRows);

        BenchmarkResult result = runBenchmark("incremental_join_" + steps + "_steps", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("users", JOIN_LEFT)
                        .registerTable("scores", JOIN_RIGHT)
                        .sql("SELECT u.name, s.score FROM users u INNER JOIN scores s ON u.id = s.id");
                engine.pushChanges("users", ZSet.fromData(JOIN_LEFT, allocator, leftData));
                engine.pushChanges("scores", ZSet.fromData(JOIN_RIGHT, allocator, rightData));
                engine.step();
                try (ZSet out = engine.getOutput()) { out.compact(); }

                // Incremental updates to right side
                for (int s = 0; s < steps; s++) {
                    Object[][] delta = new Object[deltaSize][2];
                    for (int i = 0; i < deltaSize; i++) {
                        delta[i] = new Object[]{initialRows + s * deltaSize + i, 90 + i};
                    }
                    // Need matching left side for join matches
                    Object[][] leftDelta = new Object[deltaSize][2];
                    for (int i = 0; i < deltaSize; i++) {
                        leftDelta[i] = new Object[]{initialRows + s * deltaSize + i, "new_user_" + (s * deltaSize + i)};
                    }
                    engine.pushChanges("users", ZSet.fromData(JOIN_LEFT, allocator, leftDelta));
                    engine.pushChanges("scores", ZSet.fromData(JOIN_RIGHT, allocator, delta));
                    engine.step();
                    try (ZSet out = engine.getOutput()) { out.compact(); }
                }
            }
        });
        assertTrue(result.avgMs() < 60_000);
    }

    // ── Macro-benchmark: Complex pipeline ──

    @Test
    void benchComplexPipeline() {
        Object[][] data = generateTwoIntData(ROWS);
        BenchmarkResult result = runBenchmark("complex_filter_project_agg_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", TWO_INT_SCHEMA)
                        .sql("SELECT id, SUM(amount) AS total, COUNT(*) AS cnt " +
                                "FROM t WHERE amount > 500 GROUP BY id");
                engine.pushChanges("t", ZSet.fromData(TWO_INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertTrue(out.rowCount() > 0);
                }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    @Test
    void benchMultiQuerySharedScan() {
        Object[][] data = generateDeptSalaryData(ROWS);
        BenchmarkResult result = runBenchmark("multi_query_2_views_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("emp", STRING_INT_SCHEMA)
                        .sql("SELECT dept, SUM(salary) FROM emp GROUP BY dept")
                        .sql("SELECT dept, COUNT(*), AVG(salary) FROM emp GROUP BY dept");
                engine.pushChanges("emp", ZSet.fromData(STRING_INT_SCHEMA, allocator, data)).step();
                try (ZSet out0 = engine.getOutput(0)) { assertEquals(5, out0.rowCount()); }
                try (ZSet out1 = engine.getOutput(1)) { assertEquals(5, out1.rowCount()); }
            }
        });
        assertTrue(result.avgMs() < 30_000);
    }

    // ── Vectorized vs row-at-a-time comparison ──

    @Test
    void benchVectorizedVsRowAtATime() {
        // This test benchmarks row-at-a-time filter through the engine
        // and compares with vectorized filter using VectorizedExpressions
        Object[][] data = generateIntData(ROWS);

        BenchmarkResult rowAtATime = runBenchmark("row_filter_" + ROWS + "_rows", null, () -> {
            try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
                engine.registerTable("t", INT_SCHEMA)
                        .sql("SELECT val FROM t WHERE val > " + (ROWS / 2));
                engine.pushChanges("t", ZSet.fromData(INT_SCHEMA, allocator, data)).step();
                try (ZSet out = engine.getOutput()) {
                    assertTrue(out.rowCount() > 0);
                }
            }
        });

        BenchmarkResult vectorized = runBenchmark("vectorized_filter_" + ROWS + "_rows", null, () -> {
            com.invest.differential.expr.VectorizedEvaluator pred =
                    com.invest.differential.expr.VectorizedExpressions.gt(
                            com.invest.differential.expr.VectorizedExpressions.fieldRef(0),
                            com.invest.differential.expr.VectorizedExpressions.intLiteral(ROWS / 2)
                    );
            try (org.apache.arrow.vector.VectorSchemaRoot batch =
                         org.apache.arrow.vector.VectorSchemaRoot.create(INT_SCHEMA, allocator)) {
                batch.allocateNew();
                org.apache.arrow.vector.IntVector v = (org.apache.arrow.vector.IntVector) batch.getVector(0);
                for (int i = 0; i < ROWS; i++) v.setSafe(i, i);
                batch.setRowCount(ROWS);

                try (org.apache.arrow.vector.VectorSchemaRoot result =
                             com.invest.differential.expr.VectorizedExpressions.filterBatch(batch, pred, allocator)) {
                    assertTrue(result.getRowCount() > 0);
                }
            }
        });

        // Both should produce results; we just compare timings in output
        System.out.printf("[COMPARISON] Vectorized filter speedup: %.2fx%n",
                rowAtATime.avgMs() / Math.max(vectorized.avgMs(), 0.001));
    }
}
