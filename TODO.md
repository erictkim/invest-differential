# Feature Suggestions

## High Priority

### 1. Partial Aggregate Recomputation
The `IncrementalAggregateOperator` recomputes all groups on every step. Track which group keys were touched by ΔInput and only recompute affected groups — significant for large cardinality GROUP BY.

### 2. ~~RIGHT and FULL OUTER Joins~~ ✅
RIGHT and FULL OUTER joins are now fully implemented in `IncrementalJoinOperator` via integrate-diff with `unmatchedLeftMapper`/`unmatchedRightMapper`.

### 3. ~~Materialized View Snapshots~~ ✅
`getSnapshot()` now returns the current full accumulated view for consumers that need the whole result.

## Medium Priority

### 4. HAVING Clause Support
No post-aggregate filtering exists. Supporting `HAVING` (filter on aggregate results) would be a natural extension of the aggregate operator.

### 5. ~~Subqueries and Semi/Anti-Joins~~ ✅
Semi-joins (`IN (subquery)`) and anti-joins are now supported via `IncrementalJoinOperator` with `SEMI`/`ANTI` join types. Enabled Calcite subquery expansion (`withExpand(true)`) for automatic decorrelation.

### 6. LIMIT/OFFSET (Fetch)
Currently throws `UnsupportedOperationException`. While tricky for incremental semantics (requires maintained ordering), a top-K operator is feasible.

### 7. ~~Aggregate UDFs (UDAFs)~~ ✅
Only scalar UDFs exist in `UdfRegistry`. Allowing user-defined aggregate functions (`registerUdaf`) would unlock custom analytics.

### 8. ~~Multi-Query Support~~ ✅
The engine compiles a single SQL query. Supporting multiple views over the same input tables (shared scan/filter operators) would avoid redundant computation.

## Lower Priority

### 9. Persistent State / Checkpointing
Operator state (accumulated inputs in join/aggregate/window) lives only in memory. Serializing state to disk would enable recovery and larger-than-memory workloads.

### 10. Timestamp-Indexed / Arrangement-Based Traces
Move from single-timestep Z-sets to multi-timestep traces (as in Differential Dataflow), enabling out-of-order updates, late arrivals, and temporal queries.

### 11. ~~Standard String Functions~~ ✅
`UPPER`, `LOWER`, `CHAR_LENGTH`, `SUBSTRING`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `STARTS_WITH`, `CONCAT` are all implemented in `ScalarFunctionEvaluator` and tested in `StringFunctionTest`.

### 12. ~~Date/Time Functions~~ ✅
`EXTRACT` (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/DOW/DOY), `ADD_DATE_DAYS`, `SUBTRACT_DATE_DAYS` are implemented with full date/timestamp column support. Tested in `DateTimeFunctionTest`.

### 13. ~~CASE / IF-THEN-ELSE Expressions~~ ✅
Simple CASE, searched CASE, and CASE without ELSE (returns NULL) are all implemented and tested in `CaseExpressionTest`.

### 14. ~~Set Operations: EXCEPT and INTERSECT~~ ✅
`UNION ALL`, `UNION DISTINCT`, `EXCEPT`, and `INTERSECT` are all compiled via `PlanCompiler.compileSet()` and tested in `ExceptIntersectTest`.

### 15. ~~CTE (Common Table Expressions) / WITH Clauses~~ ✅
Full `WITH` clause support is implemented and tested in `CteTest` with 8 test methods.

### 16. ~~Query Plan Visualization~~ ✅
`Circuit.toDot()` exports DOT/Graphviz format with operator nodes and stream edges. Tested in `QueryPlanVisualizationTest` with 8 tests.

### 17. ~~Operator Metrics / Telemetry~~ ✅
`OperatorMetrics` tracks step count, total nanos, and rows produced per operator. `Circuit.setMetricsEnabled(true)` activates instrumentation in `step()`. Tested in `OperatorMetricsTest` with 9 tests.

### 18. ~~Vectorized Expression Evaluation~~ ✅
`VectorizedEvaluator` and `VectorizedExpressions` provide batch-level evaluation over Arrow columnar buffers. Supports arithmetic, comparison, boolean, and string operations with `filterBatch()` and `projectBatch()` utilities. ~13x speedup over row-at-a-time. Tested in `VectorizedExpressionTest` with 21 tests.

### 19. ~~Benchmarking Suite~~ ✅
JUnit-based benchmarks with warmup and measured iterations. Covers filter, project, aggregate, join (inner/left), incremental steps, complex pipelines, multi-query, and vectorized comparison. Implemented in `BenchmarkTest` with 12 benchmarks.

### 20. Expression Support in GROUP BY / PARTITION BY / ORDER BY
Currently only field references are allowed in `GROUP BY`, `PARTITION BY`, and `ORDER BY` clauses. Computed expressions (e.g., `GROUP BY YEAR(date)` or `ORDER BY a + b`) throw exceptions. Support arbitrary expressions in these positions.

### 21. ~~Additional Date/Time Functions~~ ✅
`DATE_TRUNC`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `DATE_DIFF` are all implemented in `ScalarFunctionEvaluator` with full date/timestamp support. Tested in `AdditionalDateTimeFunctionTest` with 17 tests.

### 22. COALESCE / NULLIF / NULL-safe Comparisons
`COALESCE` is implemented but `NULLIF`, `NVL`, and `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` (null-safe equality) are missing.

### 23. GROUPING SETS / ROLLUP / CUBE
Multi-level aggregation hierarchies for reporting (e.g., `GROUP BY ROLLUP(region, product)`). Requires emitting multiple aggregate groupings per input.

### 24. ~~Incremental Window Function Optimization~~ ✅
`IncrementalWindowOperator` now maintains a `partitionOutputCache` and only recomputes partitions affected by deltas. Untouched partitions are served from cache, avoiding full recomputation. All 19 existing window tests pass.

### 25. Schema Evolution
No support for altering table schemas after registration. Adding/removing/renaming columns on registered tables would require recompiling dependent operators.
