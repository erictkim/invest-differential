# invest-differential

An embedded **incremental view maintenance** engine for Java. Register tables,
compile SQL queries into a dataflow circuit, and stream change-sets (deltas)
through the circuit — only the *changes* are recomputed, never the full result.

Built on:

- **Z-set algebra** — every row carries an integer weight; positive = insert,
  negative = delete. Updates are modeled as a delete + insert.
- **Apache Arrow 17** for columnar in-memory representation.
- **Substrait 0.44 + Calcite 1.37** for SQL-to-plan compilation.
- **parquet-floor 1.51** for Hadoop-free Parquet output with bitemporal
  `start_time` / `end_time` columns.

---

## Table of Contents

- [Install](#install)
- [Concepts in 60 seconds](#concepts-in-60-seconds)
- [Quick start](#quick-start)
- [Building deltas with `ZSet`](#building-deltas-with-zset)
- [Joins](#joins)
- [Aggregations and group-by](#aggregations-and-group-by)
- [Multiple views](#multiple-views)
- [User-defined functions](#user-defined-functions)
  - [Scalar UDFs](#scalar-udfs)
  - [Aggregate UDAFs](#aggregate-udafs)
- [Window functions](#window-functions)
- [Bitemporal Parquet output](#bitemporal-parquet-output)
- [Parallel execution and metrics](#parallel-execution-and-metrics)
- [Memory management](#memory-management)
- [API reference cheat sheet](#api-reference-cheat-sheet)

---

## Install

This project is currently a Maven `1.0-SNAPSHOT`. Build and install locally:

```bash
mvn install
```

Then depend on it from another project:

```xml
<dependency>
    <groupId>com.invest</groupId>
    <artifactId>invest-differential</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Requires **Java 17+**.

---

## Concepts in 60 seconds

A **`ZSet`** is a multiset of rows, each with a signed integer weight:

| row              | weight |
| ---------------- | -----: |
| `(1, "Alice")`   |     +1 |
| `(2, "Bob")`     |     +1 |
| `(3, "Charlie")` |     -1 |

Pushing this to a table means *insert Alice, insert Bob, delete Charlie*.
Every operator in the engine speaks this language, so updates are just
deltas you `add` together.

The **`IncrementalEngine`** lifecycle is always:

1. `create` an engine.
2. `registerTable` for each input.
3. (Optionally) `registerUdf` / `registerUdaf`.
4. `sql(...)` (one or more times) to compile views.
5. Loop: `pushChanges(table, delta).step()` then `getOutput()` /
   `getSnapshot()` to read results.
6. `close()` (use try-with-resources).

---

## Quick start

```java
import com.invest.differential.IncrementalEngine;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

try (var allocator = new RootAllocator();
     var engine = IncrementalEngine.create(allocator)) {

    Schema orders = new Schema(List.of(
        Field.notNullable("order_id", new ArrowType.Int(32, true)),
        Field.notNullable("customer", new ArrowType.Utf8()),
        Field.notNullable("amount",   new ArrowType.Int(32, true))
    ));

    engine.registerTable("orders", orders)
          .sql("SELECT customer, amount FROM orders WHERE amount > 100");

    // Initial load
    engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
            {1, "Alice",   999},
            {2, "Bob",      25},   // filtered out
            {3, "Charlie", 349},
    })).step();

    try (ZSet delta = engine.getOutput()) {
        delta.compact();
        // delta contains: (Alice, 999, +1), (Charlie, 349, +1)
    }

    // Incremental update — only the new row is processed
    engine.pushChanges("orders", ZSet.fromData(orders, allocator, new Object[][]{
            {4, "Diana", 450},
    })).step();

    try (ZSet delta = engine.getOutput()) {
        delta.compact();
        // delta contains only: (Diana, 450, +1)
    }
}
```

---

## Building deltas with `ZSet`

The simplest path is `ZSet.fromData(schema, allocator, Object[][])`, which
treats every row as weight `+1`:

```java
ZSet inserts = ZSet.fromData(schema, allocator, new Object[][]{
    {1, "Alice"},
    {2, "Bob"},
});
```

To **delete** previously inserted rows, build the same insert and call
`negate()`:

```java
ZSet deletion;
try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{2, "Bob"}})) {
    deletion = src.negate(); // independent copy with weight -1
}
engine.pushChanges("users", deletion).step();
```

> ⚠️ `negate()` clones the underlying buffers, so you must close *both* the
> source and the negated copy. Wrap the source in try-with-resources and
> hand the negated value to `pushChanges` (which then owns it).

To **update** a row, push a single delta containing both a delete and the
new insert:

```java
ZSet update;
try (ZSet oldRow = ZSet.fromData(schema, allocator, new Object[][]{{1, "Alice"}});
     ZSet newRow = ZSet.fromData(schema, allocator, new Object[][]{{1, "Alicia"}})) {
    update = oldRow.negate().add(newRow);
}
engine.pushChanges("users", update).step();
```

Useful `ZSet` operations: `add`, `subtract`, `negate`, `multiply(int)`,
`filter`, `map`, `distinct`, `compact` (drop zero-weight entries),
`union`, `unionAll`, `except`, `intersect`.

---

## Joins

Joins, sub-queries (IN / EXISTS / NOT IN), and CTEs all compile from
standard SQL — no special API:

```java
engine.registerTable("orders",   ordersSchema)
      .registerTable("customers", customersSchema)
      .sql("""
          SELECT o.order_id, c.name, o.amount
          FROM orders o
          JOIN customers c ON o.customer_id = c.id
          WHERE o.amount > 100
          """);
```

---

## Aggregations and group-by

```java
engine.registerTable("sales", salesSchema)
      .sql("""
          SELECT region, COUNT(*) AS n, SUM(amount) AS total
          FROM sales
          GROUP BY region
          """);

engine.pushChanges("sales", initialBatch).step();

// getSnapshot returns the full materialized result, not just the delta
try (ZSet snapshot = engine.getSnapshot()) {
    snapshot.compact();
    // ... iterate rows
}
```

The output of an aggregating query contains *deltas* of group rows: when a
region's total changes from 500 to 600, the delta will be
`(region, 500) → -1`, `(region, 600) → +1`.

---

## Multiple views

Compile any number of queries against the same engine. Use the named
overload of `sql(...)` to fetch results by name:

```java
engine.registerTable("orders", ordersSchema)
      .sql("SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer",
           "totals_by_customer")
      .sql("SELECT customer FROM orders WHERE amount > 1000",
           "big_spenders");

engine.pushChanges("orders", batch).step();

try (ZSet totals = engine.getSnapshot("totals_by_customer");
     ZSet big    = engine.getOutput("big_spenders")) {
    // ...
}
```

A later `sql(...)` may reference an earlier named view as if it were a
table.

---

## User-defined functions

UDFs and UDAFs must be registered **before** the first `sql(...)` call.

### Scalar UDFs

```java
import com.invest.differential.udf.ScalarUdf;

ScalarUdf strReverse = args -> {
    if (args[0] == null) return null;
    return new StringBuilder(args[0].toString()).reverse().toString();
};

engine.registerTable("t", schema)
      .registerUdf("str_reverse", strReverse, new String[]{"string"}, "string")
      .sql("SELECT STR_REVERSE(name), val FROM t");
```

Type strings are Substrait names: `"string"`, `"i32"`, `"i64"`, `"fp64"`,
`"boolean"`.

### Aggregate UDAFs

Implement `AggregateUdf` with three methods:

```java
import com.invest.differential.udf.AggregateUdf;

AggregateUdf weightedSum = new AggregateUdf() {
    @Override public Object initialize() { return 0L; }
    @Override public Object accumulate(Object acc, Object value, int weight) {
        if (value == null) return acc;
        return ((Number) acc).longValue() + ((Number) value).longValue() * weight;
    }
    @Override public Object finalize(Object acc) { return acc; }
};

engine.registerTable("t", schema)
      .registerUdaf("wsum", weightedSum, "i32", "i64")
      .sql("SELECT grp, WSUM(val) FROM t GROUP BY grp");
```

The `weight` argument lets a UDAF be **weight-aware** — it sees deletes as
negative weights, so it can correctly maintain its accumulator
incrementally.

---

## Window functions

Both `ROWS` and `RANGE` window frames are supported (use `Int32` partition
keys with `RANGE`):

```java
engine.registerTable("ticks", ticksSchema)
      .sql("""
          SELECT
              symbol,
              ts,
              price,
              AVG(price) OVER (
                  PARTITION BY symbol
                  ORDER BY ts
                  RANGE BETWEEN 60 PRECEDING AND CURRENT ROW
              ) AS rolling_avg
          FROM ticks
          """);
```

---

## Bitemporal Parquet output

Stream a view (or raw input table) to Parquet with two extra `long`
columns automatically appended:

| column       | meaning                                                   |
| ------------ | --------------------------------------------------------- |
| `start_time` | logical time the row was inserted                         |
| `end_time`   | logical time the row was retracted (or engine close time) |

```java
import java.io.File;

File out = new File("orders.parquet");

try (var engine = IncrementalEngine.create(allocator)) {
    engine.registerTable("users", schema)
          .sql("SELECT id, name FROM users", "all_users")
          .writeViewToParquet("all_users", out);

    // Insert at logical time 100
    engine.pushChanges("users",
            ZSet.fromData(schema, allocator, new Object[][]{{1, "alice"}}),
            100L).step();

    // Insert at logical time 200
    engine.pushChanges("users",
            ZSet.fromData(schema, allocator, new Object[][]{{2, "bob"}}),
            200L).step();

    // Delete alice at logical time 300
    ZSet del;
    try (ZSet src = ZSet.fromData(schema, allocator, new Object[][]{{1, "alice"}})) {
        del = src.negate();
    }
    engine.pushChanges("users", del, 300L).step();

    // Advance the clock so still-live rows get end_time = 400 on close
    engine.setEventTime(400L);
}
// orders.parquet now contains:
//   (1, "alice", start=100, end=300)
//   (2, "bob",   start=200, end=400)
```

Notes:

- Use `writeTableToParquet(tableName, file)` to capture raw inputs instead
  of view outputs.
- `pushChanges(table, delta, eventTime)` and `setEventTime(t)` set the
  logical clock used for stamping. If you never set one, the engine falls
  back to `System.currentTimeMillis()`.
- Calcite uppercases SELECT identifiers — column names in the resulting
  Parquet file appear as `ID`, `NAME`, etc.
- Sinks must be attached **after** the relevant `sql(...)` call.

---

## Parallel execution and metrics

```java
engine.setParallel(true)        // use available processors
      .setMetricsEnabled(true);

// ... run steps ...

engine.getCircuit()
      .getMetrics()              // operator-level timings, row counts, etc.
      .forEach((op, m) -> System.out.println(op + ": " + m));
```

For finer control, pass a `ParallelConfig` to `setParallelConfig(...)`.

---

## Memory management

The engine uses Apache Arrow's strict allocator with leak detection. Two
rules keep tests green:

1. Always wrap the engine and the `RootAllocator` in try-with-resources.
2. Anything you receive from `getOutput` / `getSnapshot` is a copy that
   *you* own — close it.

```java
try (var allocator = new RootAllocator();
     var engine    = IncrementalEngine.create(allocator)) {

    engine.registerTable("t", schema).sql("SELECT * FROM t");
    engine.pushChanges("t", delta).step();

    try (ZSet out = engine.getOutput()) {
        // use out
    } // <-- closed here
}
```

`pushChanges` *takes ownership* of the delta you pass in; do not close it
yourself afterward.

---

## API reference cheat sheet

### `IncrementalEngine`

| Method                                                          | Purpose                                                   |
| --------------------------------------------------------------- | --------------------------------------------------------- |
| `create()` / `create(BufferAllocator)`                          | Construct an engine.                                      |
| `registerTable(name, schema)`                                   | Declare an input table.                                   |
| `registerUdf(name, impl, argTypes, returnType)`                 | Register a scalar function (pre-compile only).            |
| `registerUdaf(name, impl, argType, returnType)`                 | Register an aggregate function (pre-compile only).        |
| `sql(query)` / `sql(query, viewName)`                           | Compile a SQL query into the circuit.                     |
| `plan(plan)` / `planFromBytes(bytes)`                           | Compile a Substrait plan directly.                        |
| `pushChanges(table, delta)`                                     | Stage a delta on an input table.                          |
| `pushChanges(table, delta, eventTime)`                          | Stage a delta and set the Parquet sink clock.             |
| `setEventTime(t)` / `getEventTime()`                            | Manually advance the Parquet sink clock.                  |
| `step()`                                                        | Run one round of the dataflow.                            |
| `getOutput()` / `getOutput(int)` / `getOutput(String)`          | Read the **delta** from the last `step()`.                |
| `getSnapshot()` / `getSnapshot(int)` / `getSnapshot(String)`    | Read the **full materialized result** so far.             |
| `writeViewToParquet(viewName, file)`                            | Bitemporal Parquet sink on a compiled view.               |
| `writeTableToParquet(tableName, file)`                          | Bitemporal Parquet sink on a raw input table.             |
| `setParallel(bool)` / `setParallelConfig(cfg)`                  | Toggle parallel operator execution.                       |
| `setMetricsEnabled(bool)`                                       | Toggle per-operator metrics collection.                   |
| `reset()`                                                       | Reset all operator state.                                 |
| `close()`                                                       | Tear down the circuit and flush Parquet sinks.            |

### `ZSet`

| Method                                                 | Purpose                                              |
| ------------------------------------------------------ | ---------------------------------------------------- |
| `empty(schema, allocator)`                             | Empty Z-set.                                         |
| `fromData(schema, allocator, Object[][])`              | Build a Z-set with weight `+1` per row.              |
| `negate()` / `add(z)` / `subtract(z)` / `multiply(n)`  | Algebra.                                             |
| `filter(p)` / `map(schema, m)` / `distinct()`          | Row-level transforms.                                |
| `compact()`                                            | Drop zero-weight rows.                               |
| `union` / `unionAll` / `except` / `intersect`          | SQL-style set operations.                            |
| `rowCount()` / `getDataValues(i)` / `getWeight(i)`     | Inspection.                                          |
| `close()`                                              | Free Arrow buffers (always required).                |

---

## Running the tests

```bash
mvn test
```

The test suite (264 tests) doubles as executable documentation — see
[`src/test/java/com/invest/differential/`](src/test/java/com/invest/differential)
for end-to-end examples of every feature above:

- [DemoTest.java](src/test/java/com/invest/differential/DemoTest.java) — guided tour
- [IncrementalEngineTest.java](src/test/java/com/invest/differential/IncrementalEngineTest.java) — core engine
- [UdfTest.java](src/test/java/com/invest/differential/UdfTest.java) / [UdafTest.java](src/test/java/com/invest/differential/UdafTest.java)
- [WindowFunctionTest.java](src/test/java/com/invest/differential/WindowFunctionTest.java) / [RollingAggregateTest.java](src/test/java/com/invest/differential/RollingAggregateTest.java)
- [ParquetSerializerTest.java](src/test/java/com/invest/differential/ParquetSerializerTest.java) — bitemporal sink
- [ParallelExecutionTest.java](src/test/java/com/invest/differential/ParallelExecutionTest.java)
