# Feature Suggestions

## High Priority

### 1. Partial Aggregate Recomputation
The `IncrementalAggregateOperator` recomputes all groups on every step. Track which group keys were touched by ΔInput and only recompute affected groups — significant for large cardinality GROUP BY.

### 2. RIGHT and FULL OUTER Joins
`IncrementalJoinOperator` has a `default` case that falls through to inner join for RIGHT/FULL. Implement these properly (anti-join based) to round out join support.

### 3. Materialized View Snapshots
The engine only exposes deltas via `getOutput()`. Add a `getSnapshot()` or `materialize()` method that returns the current full view (accumulated output) for consumers that need the whole result.

## Medium Priority

### 4. HAVING Clause Support
No post-aggregate filtering exists. Supporting `HAVING` (filter on aggregate results) would be a natural extension of the aggregate operator.

### 5. ~~Subqueries and Semi/Anti-Joins~~ ✅
Semi-joins (`IN (subquery)`) and anti-joins are now supported via `IncrementalJoinOperator` with `SEMI`/`ANTI` join types. Enabled Calcite subquery expansion (`withExpand(true)`) for automatic decorrelation.

### 6. LIMIT/OFFSET (Fetch)
Currently throws `UnsupportedOperationException`. While tricky for incremental semantics (requires maintained ordering), a top-K operator is feasible.

### 7. ~~Aggregate UDFs (UDAFs)~~ ✅
Only scalar UDFs exist in `UdfRegistry`. Allowing user-defined aggregate functions (`registerUdaf`) would unlock custom analytics.

### 8. Multi-Query Support
The engine compiles a single SQL query. Supporting multiple views over the same input tables (shared scan/filter operators) would avoid redundant computation.

## Lower Priority

### 9. Persistent State / Checkpointing
Operator state (accumulated inputs in join/aggregate/window) lives only in memory. Serializing state to disk would enable recovery and larger-than-memory workloads.

### 10. Timestamp-Indexed / Arrangement-Based Traces
Move from single-timestep Z-sets to multi-timestep traces (as in Differential Dataflow), enabling out-of-order updates, late arrivals, and temporal queries.
