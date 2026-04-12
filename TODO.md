# Feature Suggestions

## High Priority

### 1. Partial Aggregate Recomputation
The `IncrementalAggregateOperator` recomputes all groups on every step. Track which group keys were touched by ΔInput and only recompute affected groups — significant for large cardinality GROUP BY.

### 2. RIGHT and FULL OUTER Joins
`IncrementalJoinOperator` has a `default` case that falls through to inner join for RIGHT/FULL. Implement these properly (anti-join based) to round out join support.

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

### 11. Standard String Functions
Only `CONCAT` and custom scalar UDFs are supported. Add built-in `UPPER()`, `LOWER()`, `LENGTH()`, `SUBSTRING()`, `TRIM()`, `LTRIM()`, `RTRIM()` to `ScalarFunctionEvaluator`.

### 12. Date/Time Functions
No date or timestamp arithmetic, `CURRENT_TIMESTAMP`, `DATE_TRUNC`, etc. Arrow has date/time vectors but no Substrait-to-execution mapping exists yet.

### 13. CASE / IF-THEN-ELSE Expressions
`IfThenEvaluator` exists but is untested. Validate and test conditional expressions in `SELECT` and `WHERE` clauses.

### 14. Set Operations: EXCEPT and INTERSECT
`UNION ALL` is implemented but `EXCEPT` (difference) and `INTERSECT` are not compiled. Both are natural Z-set operations (subtract weights / min weights).

### 15. CTE (Common Table Expressions) / WITH Clauses
`WITH name AS (SELECT ...) SELECT ... FROM name` — named intermediate views within a single query. Requires recursive plan compilation.

### 16. Query Plan Visualization
Export the Circuit as DOT (Graphviz) or similar format. Show operator names, stream connections, and cardinalities for debugging dataflow graphs.

### 17. Operator Metrics / Telemetry
Add counters for rows processed, timers for `step()` execution, and state-size tracking (bytes in accumulated input). Expose via a `CircuitMetrics` API.

### 18. Vectorized Expression Evaluation
Expressions are evaluated row-at-a-time. Batch-evaluate over Arrow columnar buffers for significant performance gains on large datasets.

### 19. Benchmarking Suite
Micro-benchmarks for key operators (join, aggregate, window) and macro-benchmarks for end-to-end incremental queries to catch performance regressions.
