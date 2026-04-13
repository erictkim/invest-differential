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
 * Tests for rolling (RANGE-based) window aggregates with incremental delta processing.
 *
 * Rolling aggregates use RANGE BETWEEN to define windows based on ORDER BY column
 * values rather than row positions, as described in Feldera's rolling aggregates design.
 */
class RollingAggregateTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private Map<List<Object>, Integer> toMap(ZSet zset) {
        Map<List<Object>, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            List<Object> row = List.of(zset.getDataValues(i));
            map.merge(row, zset.getWeight(i), Integer::sum);
        }
        return map;
    }

    private ZSet negated(Schema schema, Object[][] data) {
        ZSet pos = ZSet.fromData(schema, allocator, data);
        ZSet neg = pos.negate();
        pos.close();
        return neg;
    }

    /** Collect only positive-weight rows as a set. */
    private Set<List<Object>> positiveRows(ZSet zset) {
        Set<List<Object>> rows = new HashSet<>();
        for (int i = 0; i < zset.rowCount(); i++) {
            if (zset.getWeight(i) > 0) {
                rows.add(List.of(zset.getDataValues(i)));
            }
        }
        return rows;
    }

    // ---- Schemas ----

    /** Transactions: cc_num (partition), ts (order), amt (measure) */
    private Schema txnSchema() {
        return new Schema(List.of(
                Field.notNullable("cc_num", new ArrowType.Int(32, true)),
                Field.notNullable("ts", new ArrowType.Int(32, true)),
                Field.notNullable("amt", new ArrowType.Int(32, true))
        ));
    }

    // =======================================================================
    // Basic rolling aggregate (single batch)
    // =======================================================================

    @Test
    void rollingSum_rangePreceding() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Transactions for card 1 at times: 1, 5, 8, 15, 20
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 1, 100},
                    {1, 5, 200},
                    {1, 8, 50},
                    {1, 15, 300},
                    {1, 20, 75},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(5, rows.size());
                // ts=1:  window [max(1-10,1)..1] = [1..1]   → 100
                // ts=5:  window [max(5-10,1)..5] = [1..5]   → 100+200 = 300
                // ts=8:  window [max(8-10,1)..8] = [1..8]   → 100+200+50 = 350
                // ts=15: window [15-10..15] = [5..15]        → 200+50+300 = 550
                // ts=20: window [20-10..20] = [10..20]       → 300+75 = 375
                assertTrue(rows.contains(List.of(1, 1, 100, 100)));
                assertTrue(rows.contains(List.of(1, 5, 200, 300)));
                assertTrue(rows.contains(List.of(1, 8, 50, 350)));
                assertTrue(rows.contains(List.of(1, 15, 300, 550)));
                assertTrue(rows.contains(List.of(1, 20, 75, 375)));
            }
        }
    }

    @Test
    void rollingCount_rangePreceding() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "COUNT(*) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_count " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 1, 100},
                    {1, 3, 200},
                    {1, 6, 50},
                    {1, 10, 300},
                    {1, 20, 75},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(5, rows.size());
                // ts=1:  window [1..1]   → count=1
                // ts=3:  window [1..3]   → count=2
                // ts=6:  window [1..6]   → count=3
                // ts=10: window [5..10]  → count=2 (ts=6,10)
                // ts=20: window [15..20] → count=1 (ts=20)
                assertTrue(rows.contains(List.of(1, 1, 100, 1L)));
                assertTrue(rows.contains(List.of(1, 3, 200, 2L)));
                assertTrue(rows.contains(List.of(1, 6, 50, 3L)));
                assertTrue(rows.contains(List.of(1, 10, 300, 2L)));
                assertTrue(rows.contains(List.of(1, 20, 75, 1L)));
            }
        }
    }

    @Test
    void rollingSum_multiplePartitions() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 1, 100},
                    {1, 5, 200},
                    {2, 2, 50},
                    {2, 8, 75},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(4, rows.size());
                // Card 1: ts=1 → 100, ts=5 → 100+200=300
                // Card 2: ts=2 → 50,  ts=8 → 75 (ts=2 is out of range [3..8])
                assertTrue(rows.contains(List.of(1, 1, 100, 100)));
                assertTrue(rows.contains(List.of(1, 5, 200, 300)));
                assertTrue(rows.contains(List.of(2, 2, 50, 50)));
                assertTrue(rows.contains(List.of(2, 8, 75, 75)));
            }
        }
    }

    // =======================================================================
    // Incremental: insert new events and check deltas
    // =======================================================================

    @Test
    void rollingSum_incrementalInsert_deltaContainsRetractions() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Step 1: initial transactions
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5, 100},
                    {1, 15, 200},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                // ts=5:  window [0..5]   → 100
                // ts=15: window [5..15]  → 100+200 = 300
                assertEquals(1, rows.get(List.of(1, 5, 100, 100)));
                assertEquals(1, rows.get(List.of(1, 15, 200, 300)));
            }

            // Step 2: insert event at ts=10 (falls within window of ts=15)
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10, 50},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=10 is new: window [0..10] → 100+50 = 150
                assertEquals(1, rows.get(List.of(1, 10, 50, 150)));
                // ts=15: window [5..15] now includes ts=10 → 100+50+200 = 350
                // Old ts=15 retracted, new ts=15 added
                assertEquals(-1, rows.get(List.of(1, 15, 200, 300)));
                assertEquals(1, rows.get(List.of(1, 15, 200, 350)));
                // ts=5 unchanged (ts=10 is NOT in [0..5])
                assertNull(rows.get(List.of(1, 5, 100, 100)));
            }
        }
    }

    @Test
    void rollingSum_incrementalDelete_deltaRetracts() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Step 1: three events
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5, 100},
                    {1, 10, 50},
                    {1, 15, 200},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                // ts=5:  [0..5]   → 100
                // ts=10: [0..10]  → 100+50 = 150
                // ts=15: [5..15]  → 100+50+200 = 350
                assertEquals(1, rows.get(List.of(1, 5, 100, 100)));
                assertEquals(1, rows.get(List.of(1, 10, 50, 150)));
                assertEquals(1, rows.get(List.of(1, 15, 200, 350)));
            }

            // Step 2: delete ts=10
            engine.pushChanges("txn", negated(schema, new Object[][]{
                    {1, 10, 50},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=10 retracted
                assertEquals(-1, rows.get(List.of(1, 10, 50, 150)));
                // ts=15: window [5..15] now just 100+200 = 300
                assertEquals(-1, rows.get(List.of(1, 15, 200, 350)));
                assertEquals(1, rows.get(List.of(1, 15, 200, 300)));
                // ts=5 unchanged
                assertNull(rows.get(List.of(1, 5, 100, 100)));
            }
        }
    }

    // =======================================================================
    // Three-step incremental: insert, insert, delete
    // =======================================================================

    @Test
    void rollingSum_threeSteps_insertInsertDelete() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Step 1: card 1 at ts=0, ts=5
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 0, 100},
                    {1, 5, 200},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Set<List<Object>> rows = positiveRows(r1);
                // ts=0: [0..0] → 100
                // ts=5: [0..5] → 100+200=300
                assertTrue(rows.contains(List.of(1, 0, 100, 100)));
                assertTrue(rows.contains(List.of(1, 5, 200, 300)));
            }

            // Step 2: insert ts=12 (outside window of ts=0 and ts=5, but ts=5 is in window of ts=12)
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 12, 50},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=12: window [2..12] → 200+50 = 250
                assertEquals(1, rows.get(List.of(1, 12, 50, 250)));
                // ts=0 and ts=5 unchanged (ts=12 not in their windows)
                assertNull(rows.get(List.of(1, 0, 100, 100)));
                assertNull(rows.get(List.of(1, 5, 200, 300)));
            }

            // Step 3: delete ts=5 → affects ts=12's rolling sum
            engine.pushChanges("txn", negated(schema, new Object[][]{
                    {1, 5, 200},
            })).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // ts=5 retracted
                assertEquals(-1, rows.get(List.of(1, 5, 200, 300)));
                // ts=12: window [2..12] now just 50 → rolling_sum=50
                assertEquals(-1, rows.get(List.of(1, 12, 50, 250)));
                assertEquals(1, rows.get(List.of(1, 12, 50, 50)));
                // ts=0 unchanged
                assertNull(rows.get(List.of(1, 0, 100, 100)));
            }
        }
    }

    // =======================================================================
    // Out-of-order events (late-arriving data)
    // =======================================================================

    @Test
    void rollingSum_outOfOrderInsert() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Step 1: events at ts=10, ts=20
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10, 100},
                    {1, 20, 200},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                // ts=10: [0..10]  → 100
                // ts=20: [10..20] → 100+200=300
                assertEquals(1, rows.get(List.of(1, 10, 100, 100)));
                assertEquals(1, rows.get(List.of(1, 20, 200, 300)));
            }

            // Step 2: late-arriving event at ts=15 (between existing events)
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 15, 50},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=15: window [5..15] → 100+50 = 150
                assertEquals(1, rows.get(List.of(1, 15, 50, 150)));
                // ts=20: window [10..20] now includes ts=15 → 100+50+200 = 350
                assertEquals(-1, rows.get(List.of(1, 20, 200, 300)));
                assertEquals(1, rows.get(List.of(1, 20, 200, 350)));
                // ts=10 unchanged
                assertNull(rows.get(List.of(1, 10, 100, 100)));
            }
        }
    }

    // =======================================================================
    // Multiple rolling aggregates in one query
    // =======================================================================

    @Test
    void multipleRollingAggregates_sumAndCount() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER w as rolling_sum, " +
                       "COUNT(*) OVER w as rolling_count " +
                       "FROM txn " +
                       "WINDOW w AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW)");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 0, 100},
                    {1, 5, 200},
                    {1, 15, 50},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(3, rows.size());
                // ts=0:  window [0..0]  → sum=100, count=1
                // ts=5:  window [0..5]  → sum=300, count=2
                // ts=15: window [5..15] → sum=250, count=2
                assertTrue(rows.contains(List.of(1, 0, 100, 100, 1L)));
                assertTrue(rows.contains(List.of(1, 5, 200, 300, 2L)));
                assertTrue(rows.contains(List.of(1, 15, 50, 250, 2L)));
            }
        }
    }

    @Test
    void multipleRollingAggregates_incrementalUpdate() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER w as rolling_sum, " +
                       "COUNT(*) OVER w as rolling_count " +
                       "FROM txn " +
                       "WINDOW w AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW)");

            // Step 1: two events
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 0, 100},
                    {1, 15, 50},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Map<List<Object>, Integer> rows = toMap(r1);
                // ts=0:  sum=100, count=1
                // ts=15: sum=50, count=1 (ts=0 not in [5..15])
                assertEquals(1, rows.get(List.of(1, 0, 100, 100, 1L)));
                assertEquals(1, rows.get(List.of(1, 15, 50, 50, 1L)));
            }

            // Step 2: insert ts=10, falls in window of ts=15 but not ts=0
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10, 200},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=10: window [0..10] → sum=100+200=300, count=2
                assertEquals(1, rows.get(List.of(1, 10, 200, 300, 2L)));
                // ts=15: window [5..15] now includes ts=10 → sum=200+50=250, count=2
                assertEquals(-1, rows.get(List.of(1, 15, 50, 50, 1L)));
                assertEquals(1, rows.get(List.of(1, 15, 50, 250, 2L)));
                // ts=0 unchanged (ts=10 not in [0..0]...wait ts=10 is outside [0-10..0])
                // Actually ts=0's window is [0..0] with range 10 preceding = [-10..0], so ts=0 only
                assertNull(rows.get(List.of(1, 0, 100, 100, 1L)));
            }
        }
    }

    // =======================================================================
    // Rolling MIN/MAX
    // =======================================================================

    @Test
    void rollingMax_rangePreceding() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "MAX(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_max " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 1, 10},
                    {1, 3, 30},
                    {1, 6, 20},
                    {1, 10, 5},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                assertEquals(4, rows.size());
                // ts=1:  [1..1]   → max=10
                // ts=3:  [1..3]   → max=30
                // ts=6:  [1..6]   → max=30
                // ts=10: [5..10]  → max=max(20,5)=20
                assertTrue(rows.contains(List.of(1, 1, 10, 10)));
                assertTrue(rows.contains(List.of(1, 3, 30, 30)));
                assertTrue(rows.contains(List.of(1, 6, 20, 30)));
                assertTrue(rows.contains(List.of(1, 10, 5, 20)));
            }
        }
    }

    @Test
    void rollingMin_incrementalInsertAndDelete() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "MIN(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_min " +
                       "FROM txn");

            // Step 1
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 1, 50},
                    {1, 4, 30},
                    {1, 8, 40},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Set<List<Object>> rows = positiveRows(r1);
                // ts=1: min=50, ts=4: min=30, ts=8: min=min(30,40)=30
                assertTrue(rows.contains(List.of(1, 1, 50, 50)));
                assertTrue(rows.contains(List.of(1, 4, 30, 30)));
                assertTrue(rows.contains(List.of(1, 8, 40, 30)));
            }

            // Step 2: insert ts=6 with value=10 → affects ts=8's window
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 6, 10},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=6: window [1..6] → min(50,30,10)=10
                assertEquals(1, rows.get(List.of(1, 6, 10, 10)));
                // ts=8: window [3..8] now min(30,10,40)=10
                assertEquals(-1, rows.get(List.of(1, 8, 40, 30)));
                assertEquals(1, rows.get(List.of(1, 8, 40, 10)));
            }

            // Step 3: delete ts=6 (the minimum) → ts=8 reverts
            engine.pushChanges("txn", negated(schema, new Object[][]{
                    {1, 6, 10},
            })).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // ts=6 retracted
                assertEquals(-1, rows.get(List.of(1, 6, 10, 10)));
                // ts=8: window [3..8] back to min(30,40)=30
                assertEquals(-1, rows.get(List.of(1, 8, 40, 10)));
                assertEquals(1, rows.get(List.of(1, 8, 40, 30)));
            }
        }
    }

    // =======================================================================
    // Fraud detection scenario (from Feldera blog)
    // =======================================================================

    @Test
    void fraudDetection_rollingSum_multipleUpdates() {
        // Mimics the Feldera blog: credit card transactions with 1-hour rolling sum
        // Using integer timestamps (seconds), window = 3600 preceding
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) as hour_sum " +
                       "FROM txn");

            // Step 1: normal spending on card 1001
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1001, 1000, 25},   // coffee
                    {1001, 2000, 50},   // lunch
                    {1001, 3000, 100},  // groceries
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Set<List<Object>> rows = positiveRows(r1);
                // All within 1 hour of each other
                // ts=1000: [0..1000]    → 25
                // ts=2000: [0..2000]    → 25+50=75
                // ts=3000: [0..3000]    → 25+50+100=175
                assertTrue(rows.contains(List.of(1001, 1000, 25, 25)));
                assertTrue(rows.contains(List.of(1001, 2000, 50, 75)));
                assertTrue(rows.contains(List.of(1001, 3000, 100, 175)));
            }

            // Step 2: suspicious activity - large charge 2 hours later
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1001, 7200, 5000},  // expensive purchase, 2hrs after first
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // ts=7200: window [3600..7200] → only ts=7200 itself = 5000
                // (all earlier transactions are more than 3600 seconds before)
                assertEquals(1, rows.get(List.of(1001, 7200, 5000, 5000)));
                // No earlier rows change
            }

            // Step 3: more charges right after the big one
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1001, 7500, 3000},  // another large charge
                    {1001, 7800, 2000},  // and another
            })).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                // ts=7500: window [3900..7500] → 5000+3000 = 8000
                assertEquals(1, rows.get(List.of(1001, 7500, 3000, 8000)));
                // ts=7800: window [4200..7800] → 5000+3000+2000 = 10000
                assertEquals(1, rows.get(List.of(1001, 7800, 2000, 10000)));
            }

            // Step 4: a refund (deletion) on the big charge — fraud reversed
            engine.pushChanges("txn", negated(schema, new Object[][]{
                    {1001, 7200, 5000},
            })).step();

            try (ZSet r4 = engine.getOutput()) {
                r4.compact();
                Map<List<Object>, Integer> rows = toMap(r4);
                // ts=7200 retracted
                assertEquals(-1, rows.get(List.of(1001, 7200, 5000, 5000)));
                // ts=7500: rolling sum drops from 8000 to 3000
                assertEquals(-1, rows.get(List.of(1001, 7500, 3000, 8000)));
                assertEquals(1, rows.get(List.of(1001, 7500, 3000, 3000)));
                // ts=7800: rolling sum drops from 10000 to 5000
                assertEquals(-1, rows.get(List.of(1001, 7800, 2000, 10000)));
                assertEquals(1, rows.get(List.of(1001, 7800, 2000, 5000)));
            }
        }
    }

    // =======================================================================
    // Edge cases
    // =======================================================================

    @Test
    void rollingSum_sameTimestamp_peerGrouping() {
        // Multiple rows with the same ORDER BY value - RANGE includes all peers
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10, 100},
                    {1, 10, 200},  // same timestamp
                    {1, 15, 50},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                // ts=10 (amt=100): window [5..10] includes both ts=10 rows → 100+200=300
                // ts=10 (amt=200): same window → 300
                // ts=15: window [10..15] → 100+200+50=350
                assertTrue(rows.contains(List.of(1, 10, 100, 300)));
                assertTrue(rows.contains(List.of(1, 10, 200, 300)));
                assertTrue(rows.contains(List.of(1, 15, 50, 350)));
            }
        }
    }

    @Test
    void rollingSum_windowSmallerThanGaps() {
        // Window size is so small that each row only sees itself
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 10, 100},
                    {1, 20, 200},
                    {1, 30, 300},
            })).step();

            try (ZSet result = engine.getOutput()) {
                result.compact();
                Set<List<Object>> rows = positiveRows(result);
                // Each row's window only includes itself (gaps > 1)
                assertTrue(rows.contains(List.of(1, 10, 100, 100)));
                assertTrue(rows.contains(List.of(1, 20, 200, 200)));
                assertTrue(rows.contains(List.of(1, 30, 300, 300)));
            }
        }
    }

    @Test
    void rollingSum_emptyDelta_noChange() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5, 100},
            })).step();
            engine.getOutput().close();

            // Empty step
            engine.pushChanges("txn", ZSet.empty(schema, allocator)).step();
            try (ZSet delta = engine.getOutput()) {
                delta.compact();
                assertTrue(delta.isEmpty());
            }
        }
    }

    // =======================================================================
    // Four-step scenario: multiple cards, cross-partition independence
    // =======================================================================

    @Test
    void rollingSum_fourSteps_multipleCards() {
        Schema schema = txnSchema();
        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("txn", schema)
                  .sql("SELECT cc_num, ts, amt, " +
                       "SUM(amt) OVER (PARTITION BY cc_num ORDER BY ts " +
                       "RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_sum " +
                       "FROM txn");

            // Step 1: two cards
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 0, 100},
                    {2, 0, 500},
            })).step();

            try (ZSet r1 = engine.getOutput()) {
                r1.compact();
                Set<List<Object>> rows = positiveRows(r1);
                assertTrue(rows.contains(List.of(1, 0, 100, 100)));
                assertTrue(rows.contains(List.of(2, 0, 500, 500)));
            }

            // Step 2: add events to both cards
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 5, 200},
                    {2, 5, 300},
            })).step();

            try (ZSet r2 = engine.getOutput()) {
                r2.compact();
                Map<List<Object>, Integer> rows = toMap(r2);
                // Card 1 ts=5: window [0..5] → 100+200=300
                assertEquals(1, rows.get(List.of(1, 5, 200, 300)));
                // Card 2 ts=5: window [0..5] → 500+300=800
                assertEquals(1, rows.get(List.of(2, 5, 300, 800)));
            }

            // Step 3: add event beyond window for card 1, within window for card 2
            engine.pushChanges("txn", ZSet.fromData(schema, allocator, new Object[][]{
                    {1, 20, 50},   // window [10..20] → only 50
                    {2, 8, 100},   // window [0..8] → 500+300+100=900
            })).step();

            try (ZSet r3 = engine.getOutput()) {
                r3.compact();
                Map<List<Object>, Integer> rows = toMap(r3);
                assertEquals(1, rows.get(List.of(1, 20, 50, 50)));
                assertEquals(1, rows.get(List.of(2, 8, 100, 900)));
            }

            // Step 4: delete from card 2 at ts=0, affects ts=5 and ts=8
            engine.pushChanges("txn", negated(schema, new Object[][]{
                    {2, 0, 500},
            })).step();

            try (ZSet r4 = engine.getOutput()) {
                r4.compact();
                Map<List<Object>, Integer> rows = toMap(r4);
                // Card 2 ts=0 retracted
                assertEquals(-1, rows.get(List.of(2, 0, 500, 500)));
                // Card 2 ts=5: window [0..5] → 300 (was 800)
                assertEquals(-1, rows.get(List.of(2, 5, 300, 800)));
                assertEquals(1, rows.get(List.of(2, 5, 300, 300)));
                // Card 2 ts=8: window [0..8] → 300+100=400 (was 900)
                assertEquals(-1, rows.get(List.of(2, 8, 100, 900)));
                assertEquals(1, rows.get(List.of(2, 8, 100, 400)));
                // Card 1 unchanged
                assertNull(rows.get(List.of(1, 0, 100, 100)));
                assertNull(rows.get(List.of(1, 20, 50, 50)));
            }
        }
    }
}
