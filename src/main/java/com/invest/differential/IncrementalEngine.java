package com.invest.differential;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.operator.Circuit;
import com.invest.differential.operator.InputOperator;
import com.invest.differential.operator.OutputOperator;
import com.invest.differential.operator.Stream;
import com.invest.differential.parallel.ParallelConfig;
import com.invest.differential.plan.PlanCompiler;
import com.invest.differential.udf.AggregateUdf;
import com.invest.differential.udf.ScalarUdf;
import com.invest.differential.udf.UdfRegistry;
import com.invest.differential.zset.ZSet;
import io.substrait.isthmus.UdfSqlToSubstrait;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/**
 * Public API for the incremental view maintenance engine.
 *
 * <p>Usage:
 * <pre>{@code
 * try (IncrementalEngine engine = IncrementalEngine.create()) {
 *     engine.registerTable("orders", ordersSchema);
 *     engine.sql("SELECT product, SUM(amount) FROM orders GROUP BY product");
 *     engine.pushChanges("orders", deltaZSet);
 *     engine.step();
 *     ZSet result = engine.getOutput();
 * }
 * }</pre>
 */
public final class IncrementalEngine implements AutoCloseable {

    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final Map<String, Schema> tableSchemas = new LinkedHashMap<>();
    private final UdfRegistry udfRegistry = new UdfRegistry();
    private final Map<String, Integer> outputNames = new LinkedHashMap<>();
    private final Map<String, Stream> viewStreams = new LinkedHashMap<>();
    private final Map<String, Schema> viewSchemas = new LinkedHashMap<>();
    private Circuit circuit;
    private boolean compiled;
    private ParallelConfig parallelConfig = ParallelConfig.disabled();
    private Long currentEventTime; // most recent event time pushed via pushChanges(name, delta, eventTime)

    private IncrementalEngine(BufferAllocator allocator, boolean ownsAllocator) {
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        registerBuiltinFunctions();
    }

    /**
     * Register engine-aware built-in SQL functions:
     * <ul>
     *   <li>{@code EVENT_TIME()} — returns the engine's current event time as a
     *       {@code BIGINT} (the value last passed to
     *       {@link #pushChanges(String, ZSet, long)} or {@link #setEventTime(long)},
     *       or {@link System#currentTimeMillis()} if none was set).</li>
     * </ul>
     *
     * <p><b>Capturing per-row creation time:</b> {@code EVENT_TIME()} returns
     * the current clock at the moment the expression is evaluated. To capture
     * the time a row was originally inserted, project it in a SELECT directly
     * over the input table and reference the column downstream:
     *
     * <pre>{@code
     * engine.sql("SELECT *, EVENT_TIME() AS created_at FROM orders", "orders_v");
     * engine.sql("SELECT customer, created_at FROM orders_v WHERE amount > 100");
     * }</pre>
     *
     * The {@code created_at} column is stamped once at insert time and flows
     * through subsequent views as a plain value.
     */
    private void registerBuiltinFunctions() {
        ScalarUdf clock = args -> currentClock();
        udfRegistry.register("event_time", clock, new String[0], "i64");
    }

    /**
     * Create an engine with its own RootAllocator.
     */
    public static IncrementalEngine create() {
        return new IncrementalEngine(new RootAllocator(), true);
    }

    /**
     * Create an engine using an existing allocator (caller manages lifecycle).
     */
    public static IncrementalEngine create(BufferAllocator allocator) {
        return new IncrementalEngine(allocator, false);
    }

    /**
     * Register a table schema. Must be called before query compilation.
     */
    public IncrementalEngine registerTable(String name, Schema schema) {
        if (compiled) {
            throw new IllegalStateException("Cannot register tables after compilation");
        }
        tableSchemas.put(name, schema);
        return this;
    }

    /**
     * Register a user-defined scalar function. Must be called before query compilation.
     *
     * @param name       function name (case-insensitive in SQL)
     * @param impl       function implementation
     * @param argTypes   Substrait type names for arguments ("string", "i32", "i64", "fp64", "boolean")
     * @param returnType Substrait type name for the return value
     */
    public IncrementalEngine registerUdf(String name, ScalarUdf impl, String[] argTypes, String returnType) {
        if (compiled) {
            throw new IllegalStateException("Cannot register UDFs after compilation");
        }
        udfRegistry.register(name, impl, argTypes, returnType);
        return this;
    }

    /**
     * Register a user-defined aggregate function (UDAF). Must be called before query compilation.
     *
     * @param name       function name (case-insensitive in SQL)
     * @param impl       aggregate function implementation
     * @param argType    Substrait type name for the argument ("string", "i32", "i64", "fp64", "boolean")
     * @param returnType Substrait type name for the return value
     */
    public IncrementalEngine registerUdaf(String name, AggregateUdf impl, String argType, String returnType) {
        if (compiled) {
            throw new IllegalStateException("Cannot register UDAFs after compilation");
        }
        udfRegistry.registerUdaf(name, impl, argType, returnType);
        return this;
    }

    /**
     * Get the UDF registry (for advanced usage).
     */
    public UdfRegistry getUdfRegistry() {
        return udfRegistry;
    }

    /**
     * Compile a SQL query into the incremental circuit.
     * Can be called multiple times to add multiple views over the same input tables.
     * Each call adds one output, accessible via {@link #getOutput(int)} using the
     * order in which queries were added, or via {@link #getOutput(String)} using
     * the view name assigned by {@link #sql(String, String)}.
     */
    public IncrementalEngine sql(String sqlQuery) {
        return sql(sqlQuery, null);
    }

    /**
     * Compile a named SQL query into the incremental circuit.
     *
     * @param sqlQuery the SQL query
     * @param viewName optional name for this view (for retrieval via {@link #getOutput(String)})
     */
    public IncrementalEngine sql(String sqlQuery, String viewName) {
        try {
            // Build CREATE TABLE statements for Calcite schema
            List<String> createStatements = new ArrayList<>();
            for (Map.Entry<String, Schema> entry : tableSchemas.entrySet()) {
                createStatements.add(buildCreateTable(entry.getKey(), entry.getValue()));
            }
            // Also register existing views as virtual tables so they can be referenced
            for (Map.Entry<String, Schema> entry : viewSchemas.entrySet()) {
                createStatements.add(buildCreateTable(entry.getKey(), entry.getValue()));
            }

            io.substrait.proto.Plan protoPlan;
            io.substrait.extension.SimpleExtension.ExtensionCollection extensions;
            extensions = udfRegistry.buildMergedExtensions();
            UdfSqlToSubstrait converter = new UdfSqlToSubstrait(
                    udfRegistry.buildOperatorTable(),
                    udfRegistry.buildSigs(),
                    udfRegistry.buildAggSigs(),
                    extensions);
            protoPlan = converter.execute(sqlQuery, createStatements);

            ProtoPlanConverter planConverter = new ProtoPlanConverter(extensions);
            Plan plan = planConverter.from(protoPlan);
            return addPlan(plan, viewName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile SQL: " + sqlQuery, e);
        }
    }

    /**
     * Compile a Substrait plan (POJO) into the incremental circuit.
     * For single-query usage. For multi-query, use {@link #sql(String)} multiple times.
     */
    public IncrementalEngine plan(Plan plan) {
        return addPlan(plan, null);
    }

    private IncrementalEngine addPlan(Plan plan, String viewName) {
        if (circuit == null) {
            circuit = new Circuit();
            circuit.setParallelConfig(parallelConfig);
        }
        int outputsBefore = circuit.getOutputs().size();
        PlanCompiler compiler = new PlanCompiler(allocator, tableSchemas, udfRegistry, circuit, viewStreams);
        compiler.compile(plan);
        this.compiled = true;
        // Propagate parallel config to newly added operators
        circuit.setParallelConfig(parallelConfig);

        // Register view names and streams for newly added outputs
        int outputsAfter = circuit.getOutputs().size();
        List<Stream> resultStreams = compiler.getLastResultStreams();
        if (viewName != null) {
            String key = viewName.toLowerCase(java.util.Locale.ROOT);
            outputNames.put(key, outputsBefore);
            if (!resultStreams.isEmpty()) {
                viewStreams.put(key, resultStreams.get(0));
                // Build view schema with correct column names from the Substrait plan root
                Plan.Root root = plan.getRoots().get(0);
                List<String> names = root.getNames();
                Schema streamSchema = resultStreams.get(0).dataSchema();
                List<org.apache.arrow.vector.types.pojo.Field> viewFields = new ArrayList<>();
                for (int i = 0; i < names.size() && i < streamSchema.getFields().size(); i++) {
                    org.apache.arrow.vector.types.pojo.Field original = streamSchema.getFields().get(i);
                    viewFields.add(new org.apache.arrow.vector.types.pojo.Field(
                            names.get(i), original.getFieldType(), original.getChildren()));
                }
                viewSchemas.put(key, new Schema(viewFields));
            }
        }
        // Auto-generate names for unnamed views
        for (int i = outputsBefore; i < outputsAfter; i++) {
            String autoName = "view_" + i;
            if (!outputNames.containsValue(i)) {
                outputNames.putIfAbsent(autoName, i);
            }
        }
        return this;
    }

    /**
     * Compile a Substrait plan from protobuf bytes.
     */
    public IncrementalEngine planFromBytes(byte[] protobufBytes) {
        try {
            io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.parseFrom(protobufBytes);
            ProtoPlanConverter converter = udfRegistry.isEmpty()
                    ? new ProtoPlanConverter()
                    : new ProtoPlanConverter(udfRegistry.buildMergedExtensions());
            Plan plan = converter.from(protoPlan);
            return plan(plan);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Substrait plan from bytes", e);
        }
    }

    /**
     * Push a delta (change set) for a table.
     * Takes ownership of the delta — the caller must not use it after this call.
     */
    public IncrementalEngine pushChanges(String tableName, ZSet delta) {
        ensureCompiled();
        InputOperator input = circuit.getInput(tableName);
        if (input == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        input.setValue(delta);
        return this;
    }

    /**
     * Push a delta with an explicit event time. The event time becomes the
     * current logical timestamp used by Parquet sinks (see
     * {@link #writeViewToParquet(String, java.io.File)}) for {@code start_time}
     * and {@code end_time} stamping until the next call that supplies an event
     * time. If no event time has ever been supplied, sinks fall back to
     * {@link System#currentTimeMillis()}.
     */
    public IncrementalEngine pushChanges(String tableName, ZSet delta, long eventTime) {
        this.currentEventTime = eventTime;
        return pushChanges(tableName, delta);
    }

    /**
     * Set the current event time used by Parquet sinks for the next steps.
     * Useful when no input changes are being pushed but a logical time advance
     * is required (e.g., to stamp {@code end_time} when the engine closes).
     */
    public IncrementalEngine setEventTime(long eventTime) {
        this.currentEventTime = eventTime;
        return this;
    }

    /**
     * Get the most recently set event time, or {@code null} if none has been set.
     */
    public Long getEventTime() {
        return currentEventTime;
    }

    private long currentClock() {
        Long t = currentEventTime;
        return t != null ? t : System.currentTimeMillis();
    }

    /**
     * Execute one step of the incremental circuit.
     * Processes all pending deltas through the operator graph.
     */
    public IncrementalEngine step() {
        ensureCompiled();
        circuit.step();
        return this;
    }

    /**
     * Get the output delta from the last step.
     */
    public ZSet getOutput() {
        return getOutput(0);
    }

    /**
     * Get the output delta from a specific output index.
     */
    public ZSet getOutput(int index) {
        ensureCompiled();
        List<OutputOperator> outputs = circuit.getOutputs();
        if (index >= outputs.size()) {
            throw new IndexOutOfBoundsException("Output index " + index + " out of range (size=" + outputs.size() + ")");
        }
        ZSet val = outputs.get(index).getValue();
        if (val == null) {
            return ZSet.empty(outputs.get(index).getOutput().dataSchema(), allocator);
        }
        return ZSet.fromRoot(val.dataSchema(),
                ArrowUtils.cloneRoot(val.data(), allocator), allocator);
    }

    /**
     * Get the output delta for a named view.
     *
     * @param viewName the view name assigned via {@link #sql(String, String)}
     */
    public ZSet getOutput(String viewName) {
        return getOutput(resolveViewIndex(viewName));
    }

    /**
     * Get the full materialized snapshot of the view — the accumulated result of all
     * output deltas across all steps, compacted to remove zero-weight entries.
     *
     * <p>The returned ZSet is a copy; the caller must close it when done.
     */
    public ZSet getSnapshot() {
        return getSnapshot(0);
    }

    /**
     * Get the full materialized snapshot from a specific output index.
     */
    public ZSet getSnapshot(int index) {
        ensureCompiled();
        List<OutputOperator> outputs = circuit.getOutputs();
        if (index >= outputs.size()) {
            throw new IndexOutOfBoundsException("Output index " + index + " out of range (size=" + outputs.size() + ")");
        }
        ZSet snapshot = outputs.get(index).getSnapshot();
        return ZSet.fromRoot(snapshot.dataSchema(),
                ArrowUtils.cloneRoot(snapshot.data(), allocator), allocator);
    }

    /**
     * Get the full materialized snapshot for a named view.
     *
     * @param viewName the view name assigned via {@link #sql(String, String)}
     */
    public ZSet getSnapshot(String viewName) {
        return getSnapshot(resolveViewIndex(viewName));
    }

    /**
     * Get the number of compiled outputs (views).
     */
    public int getOutputCount() {
        return circuit != null ? circuit.getOutputs().size() : 0;
    }

    /**
     * Stream the deltas of a registered view to a Parquet file with bitemporal
     * {@code start_time} and {@code end_time} columns. Each row that has been
     * inserted but not yet retracted is flushed to the file when the engine
     * closes.
     *
     * <p>Must be called after the view's query has been compiled via
     * {@link #sql(String, String)}.
     */
    public IncrementalEngine writeViewToParquet(String viewName, java.io.File outFile) {
        ensureCompiled();
        String key = viewName.toLowerCase(java.util.Locale.ROOT);
        com.invest.differential.operator.Stream stream = viewStreams.get(key);
        Schema schema = viewSchemas.get(key);
        if (stream == null || schema == null) {
            throw new IllegalArgumentException("Unknown view: " + viewName);
        }
        return attachParquetSink(viewName, stream, schema, outFile);
    }

    /**
     * Stream the deltas of a registered input table to a Parquet file with
     * bitemporal {@code start_time} and {@code end_time} columns. Live rows
     * are flushed to the file when the engine closes.
     *
     * <p>Must be called after at least one query has been compiled (so the
     * input operator exists in the circuit).
     */
    public IncrementalEngine writeTableToParquet(String tableName, java.io.File outFile) {
        ensureCompiled();
        InputOperator input = circuit.getInput(tableName);
        if (input == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        Schema schema = tableSchemas.get(tableName);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return attachParquetSink(tableName, input.getOutput(), schema, outFile);
    }

    private IncrementalEngine attachParquetSink(String name,
                                                com.invest.differential.operator.Stream stream,
                                                Schema schema,
                                                java.io.File outFile) {
        try {
            com.invest.differential.io.ParquetSerializer serializer =
                    com.invest.differential.io.ParquetSerializer.create(schema, outFile, this::currentClock);
            com.invest.differential.io.ParquetSinkOperator sink =
                    new com.invest.differential.io.ParquetSinkOperator(name, stream, serializer);
            circuit.addOperator(sink);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to open Parquet writer for " + outFile, e);
        }
        return this;
    }

    /**
     * Attach an in-memory bitemporal accumulator sink to a registered view.
     * The returned {@link com.invest.differential.io.AccumulatorSinkOperator}
     * collects one {@link com.invest.differential.io.BitemporalAccumulator}
     * record per row that is created and either later retracted or still live
     * when the engine closes. The sink's
     * {@link com.invest.differential.io.AccumulatorSinkOperator#schema()}
     * exposes the Arrow schema (column names + types) for each row's values.
     *
     * <p>Must be called after the view's query has been compiled via
     * {@link #sql(String, String)}.
     */
    public com.invest.differential.io.AccumulatorSinkOperator accumulateView(String viewName) {
        ensureCompiled();
        String key = viewName.toLowerCase(java.util.Locale.ROOT);
        com.invest.differential.operator.Stream stream = viewStreams.get(key);
        Schema schema = viewSchemas.get(key);
        if (stream == null || schema == null) {
            throw new IllegalArgumentException("Unknown view: " + viewName);
        }
        return attachAccumulatorSink(viewName, stream, schema);
    }

    /**
     * Attach an in-memory bitemporal accumulator sink to a registered input
     * table. See {@link #accumulateView(String)} for semantics.
     */
    public com.invest.differential.io.AccumulatorSinkOperator accumulateTable(String tableName) {
        ensureCompiled();
        InputOperator input = circuit.getInput(tableName);
        if (input == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        Schema schema = tableSchemas.get(tableName);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return attachAccumulatorSink(tableName, input.getOutput(), schema);
    }

    private com.invest.differential.io.AccumulatorSinkOperator attachAccumulatorSink(
            String name,
            com.invest.differential.operator.Stream stream,
            Schema schema) {
        com.invest.differential.io.AccumulatorSinkOperator sink =
                new com.invest.differential.io.AccumulatorSinkOperator(
                        name, stream, schema, this::currentClock);
        circuit.addOperator(sink);
        return sink;
    }

    /**
     * Reset all operator state.
     */
    public IncrementalEngine reset() {
        if (circuit != null) {
            circuit.reset();
        }
        return this;
    }

    /**
     * Get the allocator used by this engine.
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Get the compiled circuit (for advanced inspection).
     */
    public Circuit getCircuit() {
        return circuit;
    }

    /**
     * Enable or disable operator-level metrics collection.
     */
    public IncrementalEngine setMetricsEnabled(boolean enabled) {
        if (circuit != null) {
            circuit.setMetricsEnabled(enabled);
        }
        return this;
    }

    /**
     * Enable parallel execution with default settings (uses available processors).
     */
    public IncrementalEngine setParallel(boolean enabled) {
        this.parallelConfig = enabled ? ParallelConfig.withDefaults() : ParallelConfig.disabled();
        if (circuit != null) {
            circuit.setParallelConfig(parallelConfig);
        }
        return this;
    }

    /**
     * Set a specific parallel configuration. Pass {@code ParallelConfig.disabled()} to disable.
     */
    public IncrementalEngine setParallelConfig(ParallelConfig config) {
        this.parallelConfig = config != null ? config : ParallelConfig.disabled();
        if (circuit != null) {
            circuit.setParallelConfig(parallelConfig);
        }
        return this;
    }

    /**
     * Get the current parallel configuration.
     */
    public ParallelConfig getParallelConfig() {
        return parallelConfig;
    }

    @Override
    public void close() {
        if (circuit != null) {
            circuit.close();
        }
        if (parallelConfig != null) {
            parallelConfig.shutdown();
        }
        if (ownsAllocator) {
            allocator.close();
        }
    }

    private void ensureCompiled() {
        if (!compiled) {
            throw new IllegalStateException("No query compiled yet. Call sql() or plan() first.");
        }
    }

    private int resolveViewIndex(String viewName) {
        Integer index = outputNames.get(viewName.toLowerCase(java.util.Locale.ROOT));
        if (index == null) {
            throw new IllegalArgumentException("Unknown view: " + viewName);
        }
        return index;
    }

    private String buildCreateTable(String name, Schema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(name).append(" (");
        List<org.apache.arrow.vector.types.pojo.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            org.apache.arrow.vector.types.pojo.Field field = fields.get(i);
            sb.append("\"").append(field.getName()).append("\" ");
            sb.append(arrowTypeToSql(field.getType()));
            if (!field.isNullable()) {
                sb.append(" NOT NULL");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String arrowTypeToSql(org.apache.arrow.vector.types.pojo.ArrowType type) {
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int intType) {
            return switch (intType.getBitWidth()) {
                case 8 -> "TINYINT";
                case 16 -> "SMALLINT";
                case 32 -> "INTEGER";
                case 64 -> "BIGINT";
                default -> "INTEGER";
            };
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> "REAL";
                case DOUBLE -> "DOUBLE";
                default -> "DOUBLE";
            };
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
            return "VARCHAR";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
            return "BOOLEAN";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Decimal dec) {
            return "DECIMAL(" + dec.getPrecision() + "," + dec.getScale() + ")";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
            return "DATE";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            return "TIMESTAMP";
        }
        return "VARCHAR";
    }
}
