package com.invest.differential;

import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL window functions (OVER clause) with incremental delta processing.
 */
class WindowFunctionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /** Collect a ZSet into a list of row-as-list for easy assertion. */
    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }

    /** Collect positive-weight rows from a ZSet into a set of row-lists. */
    private Set<List<Object>> positiveRows(ZSet zset) {
        Set<List<Object>> rows = new HashSet<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            if (zset.getWeight(i) > 0) {
                rows.add(List.of(zset.getDataValues(i)));
            }
        }
        return rows;
    }

    /** Create a negated ZSet (for deletions) without leaking the intermediate. */
    private ZSet negated(Schema schema, Object[][] data) {
        ZSet pos = ZSet.fromData(schema, allocator, data);
        ZSet neg = pos.negate();
        pos.close();
        return neg;
    }

    // ---- Schema helpers ----

    private Schema salesSchema() {
        return new Schema(List.of(
                Field.notNullable("dept", new ArrowType.Utf8()),
                Field.notNullable("emp", new ArrowType.Utf8()),
                Field.notNullable("salary", new ArrowType.Int(32, true))
        ));
    }

    private Schema ordersSchema() {
        return new Schema(List.of(
                Field.notNullable("customer", new ArrowType.Utf8()),
                Field.notNullable("product", new ArrowType.Utf8()),
                Field.notNullable("amount", new ArrowType.Int(32, true))
        ));
    }

    // =======================================================================
    // Basic window function tests
    // =======================================================================

    @Test
    void rowNumber_singlePartition() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Engineering", "Carol", 110},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                // Alice=120 → rn=1, Carol=110 → rn=2, Bob=100 → rn=3
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", "Carol", 110, 2L)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 3L)));
            }
        }
    }

    @Test
    void rowNumber_multiplePartitions() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Sales", "Diana", 90},
                    {"Sales", "Eve", 95},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(4, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 2L)));
                assertEquals(1, rows.get(List.of("Sales", "Eve", 95, 1L)));
                assertEquals(1, rows.get(List.of("Sales", "Diana", 90, 2L)));
            }
        }
    }

    @Test
    void sumOver_unboundedFrame() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Sales", "Diana", 90},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                // Engineering total = 220, Sales total = 90
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 220)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 220)));
                assertEquals(1, rows.get(List.of("Sales", "Diana", 90, 90)));
            }
        }
    }

    @Test
    void countOver_partition() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, COUNT(*) OVER (PARTITION BY dept) as dept_count FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Engineering", "Carol", 110},
                    {"Sales", "Diana", 90},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(4, rows.size());
                // Engineering count=3, Sales count=1
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 3L)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 3L)));
                assertEquals(1, rows.get(List.of("Engineering", "Carol", 110, 3L)));
                assertEquals(1, rows.get(List.of("Sales", "Diana", 90, 1L)));
            }
        }
    }

    // =======================================================================
    // Multi-step incremental tests (delta over multiple steps)
    // =======================================================================

    @Test
    void rowNumber_incrementalInsert() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales");

            // Step 1: initial data
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 2L)));
            }

            // Step 2: add Carol with salary=130 (becomes #1, shifting others)
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Carol", 130},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Carol=130 added at rn=1 (+1)
                // Alice=120 moves from rn=1 to rn=2 (-1 old, +1 new)
                // Bob=100 moves from rn=2 to rn=3 (-1 old, +1 new)
                assertEquals(1, rows.get(List.of("Engineering", "Carol", 130, 1L)));  // new
                assertEquals(-1, rows.get(List.of("Engineering", "Alice", 120, 1L))); // retracted old rank
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 2L)));  // new rank
                assertEquals(-1, rows.get(List.of("Engineering", "Bob", 100, 2L)));   // retracted old rank
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 3L)));    // new rank
            }
        }
    }

    @Test
    void rowNumber_incrementalDelete() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales");

            // Step 1: initial data
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Engineering", "Carol", 110},
            })).step();
            engine.getOutput().close();

            // Step 2: delete Alice (top earner, causes cascade)
            engine.pushChanges("sales", negated(schema, new Object[][]{
                    {"Engineering", "Alice", 120},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Alice removed → -1 for (Alice,120,rn=1)
                // Carol was rn=2, now rn=1: -1 old +1 new
                // Bob was rn=3, now rn=2: -1 old +1 new
                assertEquals(-1, rows.get(List.of("Engineering", "Alice", 120, 1L)));
                assertEquals(-1, rows.get(List.of("Engineering", "Carol", 110, 2L)));
                assertEquals(1, rows.get(List.of("Engineering", "Carol", 110, 1L)));
                assertEquals(-1, rows.get(List.of("Engineering", "Bob", 100, 3L)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 2L)));
            }
        }
    }

    @Test
    void sumOver_incrementalUpdate() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM sales");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 220)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 220)));
            }

            // Step 2: add Carol with salary=80 → department total changes 220→300
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Carol", 80},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Carol added with new total
                assertEquals(1, rows.get(List.of("Engineering", "Carol", 80, 300)));
                // Alice old total retracted, new total added
                assertEquals(-1, rows.get(List.of("Engineering", "Alice", 120, 220)));
                assertEquals(1, rows.get(List.of("Engineering", "Alice", 120, 300)));
                // Bob old total retracted, new total added
                assertEquals(-1, rows.get(List.of("Engineering", "Bob", 100, 220)));
                assertEquals(1, rows.get(List.of("Engineering", "Bob", 100, 300)));
            }
        }
    }

    @Test
    void sumOver_threeSteps_addAndRemove() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM sales");

            // Step 1: two employees
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 100},
                    {"Eng", "Bob", 200},
            })).step();
            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                assertEquals(2, r1.rowCount());
            }

            // Step 2: add Carol
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Carol", 50},
            })).step();
            engine.getOutput().close();

            // Step 3: remove Bob → total goes from 350 to 150
            engine.pushChanges("sales", negated(schema, new Object[][]{
                    {"Eng", "Bob", 200},
            })).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // Bob retracted
                assertEquals(-1, rows.get(List.of("Eng", "Bob", 200, 350)));
                // Alice and Carol updated from 350 to 150
                assertEquals(-1, rows.get(List.of("Eng", "Alice", 100, 350)));
                assertEquals(1, rows.get(List.of("Eng", "Alice", 100, 150)));
                assertEquals(-1, rows.get(List.of("Eng", "Carol", 50, 350)));
                assertEquals(1, rows.get(List.of("Eng", "Carol", 50, 150)));
            }
        }
    }

    @Test
    void rowNumber_emptyDelta_noChange() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 100},
            })).step();
            engine.getOutput().close();

            // Empty step
            engine.pushChanges("sales", ZSet.empty(schema, allocator)).step();
            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                assertTrue(delta.isEmpty());
            }
        }
    }

    // =======================================================================
    // Combined operator tests (window + other operators)
    // =======================================================================

    @Test
    void windowWithFilter_filterAfterWindow() {
        // SELECT * FROM (SELECT dept, emp, salary, ROW_NUMBER() OVER (...) as rn FROM sales) WHERE rn = 1
        // i.e., top earner per department
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, rn FROM (" +
                       "  SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales" +
                       ") WHERE rn = 1");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Engineering", "Alice", 120},
                    {"Engineering", "Bob", 100},
                    {"Sales", "Diana", 90},
                    {"Sales", "Eve", 95},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(2, rows.size());
                assertTrue(rows.contains(List.of("Engineering", "Alice", 120, 1L)));
                assertTrue(rows.contains(List.of("Sales", "Eve", 95, 1L)));
            }
        }
    }

    @Test
    void windowWithFilter_incrementalTopChange() {
        // "Top 1 per department" query, then new top earner arrives
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, rn FROM (" +
                       "  SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales" +
                       ") WHERE rn = 1");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120},
                    {"Eng", "Bob", 100},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120, 1L)));
            }

            // Step 2: Carol arrives with higher salary
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Carol", 200},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Alice retracted from rn=1, Carol takes rn=1
                assertEquals(-1, rows.get(List.of("Eng", "Alice", 120, 1L)));
                assertEquals(1, rows.get(List.of("Eng", "Carol", 200, 1L)));
            }
        }
    }

    @Test
    void windowWithAggregate_sumOverThenGroupBy() {
        // Window + aggregate: "per department, count of employees with above-avg salary"
        // Simplified: group the window results
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, SUM(salary) as total_salary, COUNT(*) as emp_count FROM (" +
                       "  SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales" +
                       ") GROUP BY dept");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120},
                    {"Eng", "Bob", 100},
                    {"Sales", "Diana", 90},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Eng", 220, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 90, 1L)));
            }
        }
    }

    @Test
    void windowWithJoin_joinThenWindow() {
        // Join two tables, then apply window function
        Schema empSchema = new Schema(List.of(
                Field.notNullable("emp_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("salary", new ArrowType.Int(32, true))
        ));
        Schema deptSchema = new Schema(List.of(
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("employees", empSchema)
                  .registerTable("departments", deptSchema)
                  .sql("SELECT dept_name, salary, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary DESC) as rn " +
                       "FROM employees e JOIN departments d ON e.dept_id = d.dept_id");

            engine.pushChanges("departments", ZSet.fromData(deptSchema, allocator, new Object[][]{
                    {1, "Engineering"},
                    {2, "Sales"},
            }));

            engine.pushChanges("employees", ZSet.fromData(empSchema, allocator, new Object[][]{
                    {101, 1, 120},
                    {102, 1, 100},
                    {103, 2, 90},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", 100, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 90, 1L)));
            }
        }
    }

    @Test
    void window_multipleWindowFunctions() {
        // Multiple window functions in a single query
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, " +
                       "  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn, " +
                       "  SUM(salary) OVER (PARTITION BY dept) as dept_total " +
                       "FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120},
                    {"Eng", "Bob", 100},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(2, rows.size());
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120, 1L, 220)));
                assertEquals(1, rows.get(List.of("Eng", "Bob", 100, 2L, 220)));
            }
        }
    }

    @Test
    void runningSum_orderByWithFrame() {
        // Running sum: SUM(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, " +
                       "SUM(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum " +
                       "FROM sales");

            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Bob", 100},
                    {"Eng", "Alice", 120},
                    {"Eng", "Carol", 80},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Map<List<Object>, Integer> rows = toMap(result);
                assertEquals(3, rows.size());
                // Ordered by salary ASC: Carol=80, Bob=100, Alice=120
                // Running sums: 80, 180, 300
                assertEquals(1, rows.get(List.of("Eng", "Carol", 80, 80)));
                assertEquals(1, rows.get(List.of("Eng", "Bob", 100, 180)));
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120, 300)));
            }
        }
    }

    @Test
    void runningSum_incrementalStep() {
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary, " +
                       "SUM(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum " +
                       "FROM sales");

            // Step 1
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Bob", 100},
                    {"Eng", "Alice", 120},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                // Bob=100 → running=100, Alice=120 → running=220
                assertEquals(1, rows.get(List.of("Eng", "Bob", 100, 100)));
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120, 220)));
            }

            // Step 2: add Carol with salary=80 (inserted before Bob in order)
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Carol", 80},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // New order: Carol=80(→80), Bob=100(→180), Alice=120(→300)
                // Carol is new: +1
                assertEquals(1, rows.get(List.of("Eng", "Carol", 80, 80)));
                // Bob changed from running=100 to running=180
                assertEquals(-1, rows.get(List.of("Eng", "Bob", 100, 100)));
                assertEquals(1, rows.get(List.of("Eng", "Bob", 100, 180)));
                // Alice changed from running=220 to running=300
                assertEquals(-1, rows.get(List.of("Eng", "Alice", 120, 220)));
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120, 300)));
            }
        }
    }

    // =======================================================================
    // Combined operator multi-step tests (window + filter/join/agg, ≥3 steps)
    // =======================================================================

    @Test
    void filterAfterWindow_threeSteps() {
        // Top-1 per department: filter rn=1 after ROW_NUMBER window
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, emp, salary FROM (" +
                       "  SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales" +
                       ") WHERE rn = 1");

            // Step 1: seed two departments
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120},
                    {"Eng", "Bob", 100},
                    {"Sales", "Diana", 90},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120)));
                assertEquals(1, rows.get(List.of("Sales", "Diana", 90)));
                assertEquals(2, rows.size());
            }

            // Step 2: new top earner in Eng; new Sales employee (not top)
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Carol", 200},
                    {"Sales", "Eve", 80},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Eng top changes Alice→Carol
                assertEquals(-1, rows.get(List.of("Eng", "Alice", 120)));
                assertEquals(1, rows.get(List.of("Eng", "Carol", 200)));
                // Sales top unchanged (Diana 90 > Eve 80), so no Sales delta
                assertNull(rows.get(List.of("Sales", "Diana", 90)));
                assertNull(rows.get(List.of("Sales", "Eve", 80)));
            }

            // Step 3: remove Carol → Alice regains top; add new Sales top
            ZSet del = negated(schema, new Object[][]{
                    {"Eng", "Carol", 200},
            });
            ZSet ins = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Sales", "Frank", 150},
            });
            ZSet combined = del.add(ins);
            del.close();
            ins.close();
            engine.pushChanges("sales", combined).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // Eng: Carol removed, Alice back to top
                assertEquals(-1, rows.get(List.of("Eng", "Carol", 200)));
                assertEquals(1, rows.get(List.of("Eng", "Alice", 120)));
                // Sales: Frank 150 > Diana 90 → top changes
                assertEquals(-1, rows.get(List.of("Sales", "Diana", 90)));
                assertEquals(1, rows.get(List.of("Sales", "Frank", 150)));
            }
        }
    }

    @Test
    void joinThenWindow_threeSteps() {
        // Join employees with departments, then rank by salary within each department
        Schema empSchema = new Schema(List.of(
                Field.notNullable("emp_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("salary", new ArrowType.Int(32, true))
        ));
        Schema deptSchema = new Schema(List.of(
                Field.notNullable("dept_id", new ArrowType.Int(32, true)),
                Field.notNullable("dept_name", new ArrowType.Utf8())
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("employees", empSchema)
                  .registerTable("departments", deptSchema)
                  .sql("SELECT dept_name, salary, " +
                       "ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary DESC) as rn " +
                       "FROM employees e JOIN departments d ON e.dept_id = d.dept_id");

            // Step 1: seed departments and employees
            engine.pushChanges("departments", ZSet.fromData(deptSchema, allocator, new Object[][]{
                    {1, "Engineering"},
                    {2, "Sales"},
            }));
            engine.pushChanges("employees", ZSet.fromData(empSchema, allocator, new Object[][]{
                    {101, 1, 120},
                    {102, 1, 100},
                    {103, 2, 90},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(3, rows.size());
                assertEquals(1, rows.get(List.of("Engineering", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", 100, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 90, 1L)));
            }

            // Step 2: add a new top earner to Engineering
            engine.pushChanges("employees", ZSet.fromData(empSchema, allocator, new Object[][]{
                    {104, 1, 200},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // New emp 200 becomes rn=1, shifting 120→rn=2, 100→rn=3
                assertEquals(1, rows.get(List.of("Engineering", 200, 1L)));
                assertEquals(-1, rows.get(List.of("Engineering", 120, 1L)));
                assertEquals(1, rows.get(List.of("Engineering", 120, 2L)));
                assertEquals(-1, rows.get(List.of("Engineering", 100, 2L)));
                assertEquals(1, rows.get(List.of("Engineering", 100, 3L)));
                // Sales unchanged
                assertNull(rows.get(List.of("Sales", 90, 1L)));
            }

            // Step 3: add new department and employee; remove an Engineering employee
            engine.pushChanges("departments", ZSet.fromData(deptSchema, allocator, new Object[][]{
                    {3, "Marketing"},
            }));
            ZSet empIns = ZSet.fromData(empSchema, allocator, new Object[][]{
                    {105, 3, 110},
            });
            ZSet empDel = negated(empSchema, new Object[][]{
                    {104, 1, 200},
            });
            ZSet empCombined = empIns.add(empDel);
            empIns.close();
            empDel.close();
            engine.pushChanges("employees", empCombined).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // Marketing: new row (110, rn=1)
                assertEquals(1, rows.get(List.of("Marketing", 110, 1L)));
                // Engineering: 200 removed (was rn=1); 120 back to rn=1, 100 back to rn=2
                assertEquals(-1, rows.get(List.of("Engineering", 200, 1L)));
                assertEquals(-1, rows.get(List.of("Engineering", 120, 2L)));
                assertEquals(1, rows.get(List.of("Engineering", 120, 1L)));
                assertEquals(-1, rows.get(List.of("Engineering", 100, 3L)));
                assertEquals(1, rows.get(List.of("Engineering", 100, 2L)));
            }
        }
    }

    @Test
    void aggregateAfterWindow_threeSteps() {
        // Window then aggregate: SUM and COUNT per department after ranking
        Schema schema = salesSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("sales", schema)
                  .sql("SELECT dept, SUM(salary) as total, COUNT(*) as cnt FROM (" +
                       "  SELECT dept, emp, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM sales" +
                       ") GROUP BY dept");

            // Step 1: two departments
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Alice", 120},
                    {"Eng", "Bob", 100},
                    {"Sales", "Diana", 90},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                assertEquals(2, rows.size());
                // Eng: 120+100=220, 2 employees;  Sales: 90, 1 employee
                assertEquals(1, rows.get(List.of("Eng", 220, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 90, 1L)));
            }

            // Step 2: add employee to each department
            engine.pushChanges("sales", ZSet.fromData(schema, allocator, new Object[][]{
                    {"Eng", "Carol", 80},
                    {"Sales", "Eve", 110},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Eng: 220→300, cnt 2→3 — retract old, emit new
                assertEquals(-1, rows.get(List.of("Eng", 220, 2L)));
                assertEquals(1, rows.get(List.of("Eng", 300, 3L)));
                // Sales: 90→200, cnt 1→2
                assertEquals(-1, rows.get(List.of("Sales", 90, 1L)));
                assertEquals(1, rows.get(List.of("Sales", 200, 2L)));
            }

            // Step 3: remove Bob from Eng, add new Sales employee
            ZSet aggDel = negated(schema, new Object[][]{
                    {"Eng", "Bob", 100},
            });
            ZSet aggIns = ZSet.fromData(schema, allocator, new Object[][]{
                    {"Sales", "Frank", 50},
            });
            ZSet aggCombined = aggDel.add(aggIns);
            aggDel.close();
            aggIns.close();
            engine.pushChanges("sales", aggCombined).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // Eng: 300→200, cnt 3→2
                assertEquals(-1, rows.get(List.of("Eng", 300, 3L)));
                assertEquals(1, rows.get(List.of("Eng", 200, 2L)));
                // Sales: 200→250, cnt 2→3
                assertEquals(-1, rows.get(List.of("Sales", 200, 2L)));
                assertEquals(1, rows.get(List.of("Sales", 250, 3L)));
            }
        }
    }
}
