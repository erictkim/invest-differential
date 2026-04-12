package com.invest.differential.plan;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.expr.*;
import com.invest.differential.operator.*;
import com.invest.differential.udf.AggregateUdf;
import com.invest.differential.udf.UdfRegistry;
import com.invest.differential.zset.AggregateDescription;
import com.invest.differential.zset.RowCombiner;
import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.RowPredicate;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.WindowBound;
import io.substrait.plan.Plan;
import io.substrait.relation.*;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;
import java.util.function.Function;

/**
 * Compiles a Substrait Plan into an incremental operator Circuit.
 *
 * <p>Walks the Substrait Rel tree and creates corresponding incremental operators.
 * Linear operators (filter, project) are directly incremental.
 * Non-linear operators (join, aggregate, distinct) use state-maintaining wrappers.
 */
public final class PlanCompiler {

    private final BufferAllocator allocator;
    private final Map<String, Schema> tableSchemas;
    private final Circuit circuit;
    private final UdfRegistry udfRegistry;
    private final Map<String, Stream> viewStreams;
    private final List<Stream> lastResultStreams = new ArrayList<>();

    public PlanCompiler(BufferAllocator allocator, Map<String, Schema> tableSchemas) {
        this(allocator, tableSchemas, null, null, null);
    }

    public PlanCompiler(BufferAllocator allocator, Map<String, Schema> tableSchemas, UdfRegistry udfRegistry) {
        this(allocator, tableSchemas, udfRegistry, null, null);
    }

    public PlanCompiler(BufferAllocator allocator, Map<String, Schema> tableSchemas, UdfRegistry udfRegistry, Circuit existingCircuit) {
        this(allocator, tableSchemas, udfRegistry, existingCircuit, null);
    }

    public PlanCompiler(BufferAllocator allocator, Map<String, Schema> tableSchemas, UdfRegistry udfRegistry, Circuit existingCircuit, Map<String, Stream> viewStreams) {
        this.allocator = allocator;
        this.tableSchemas = tableSchemas;
        this.circuit = existingCircuit != null ? existingCircuit : new Circuit();
        this.udfRegistry = udfRegistry;
        this.viewStreams = viewStreams != null ? viewStreams : Map.of();
    }

    /**
     * Compile a Substrait plan into a Circuit.
     */
    public Circuit compile(Plan plan) {
        lastResultStreams.clear();
        for (Plan.Root root : plan.getRoots()) {
            Rel rel = root.getInput();
            Stream result = compileRel(rel);
            lastResultStreams.add(result);

            // Wrap with output operator
            OutputOperator outputOp = new OutputOperator("output", result, allocator);
            circuit.addOperator(outputOp);
        }
        return circuit;
    }

    /**
     * Get the result streams from the most recent {@link #compile} call.
     * Each stream corresponds to one plan root and feeds the matching OutputOperator.
     */
    public List<Stream> getLastResultStreams() {
        return lastResultStreams;
    }

    private Stream compileRel(Rel rel) {
        if (rel instanceof NamedScan scan) {
            return compileNamedScan(scan);
        } else if (rel instanceof Filter filter) {
            return compileFilter(filter);
        } else if (rel instanceof Project project) {
            return compileProject(project);
        } else if (rel instanceof Aggregate aggregate) {
            return compileAggregate(aggregate);
        } else if (rel instanceof Join join) {
            return compileJoin(join);
        } else if (rel instanceof Cross cross) {
            return compileCross(cross);
        } else if (rel instanceof io.substrait.relation.Set set) {
            return compileSet(set);
        } else if (rel instanceof ConsistentPartitionWindow window) {
            return compileWindow(window);
        } else if (rel instanceof Sort sort) {
            // Sort is a pass-through for incremental views (ordering doesn't affect correctness)
            return compileRel(sort.getInput());
        } else if (rel instanceof Fetch fetch) {
            throw new UnsupportedOperationException("LIMIT/OFFSET (Fetch) is not supported in incremental mode");
        }
        throw new UnsupportedOperationException("Unsupported relation type: " + rel.getClass().getSimpleName());
    }

    // ---- Concrete Relation Compilers ----

    private Stream compileNamedScan(NamedScan scan) {
        List<String> names = scan.getNames();
        String tableName = names.get(names.size() - 1); // use last segment

        // Check if this references a previously compiled view
        Stream viewStream = viewStreams.get(tableName.toLowerCase(java.util.Locale.ROOT));
        if (viewStream != null) {
            return viewStream;
        }

        // Reuse existing InputOperator for the same table (multi-query support)
        InputOperator existing = circuit.findInput(tableName);
        if (existing != null) {
            return existing.getOutput();
        }

        Schema schema = tableSchemas.get(tableName);
        if (schema == null) {
            // Derive from Substrait schema
            schema = namedStructToSchema(scan.getInitialSchema());
        }

        InputOperator inputOp = new InputOperator(tableName, schema, allocator);
        circuit.addOperator(inputOp);
        return inputOp.getOutput();
    }

    private Stream compileFilter(Filter filter) {
        Stream input = compileRel(filter.getInput());
        Expression condition = filter.getCondition();
        ExpressionEvaluator evaluator = compileExpression(condition, input.dataSchema());

        RowPredicate predicate = (root, rowIndex) -> {
            Object result = evaluator.evaluate(root, rowIndex);
            return result instanceof Boolean b && b;
        };

        FilterOperator filterOp = new FilterOperator(input, predicate);
        circuit.addOperator(filterOp);
        return filterOp.getOutput();
    }

    private Stream compileProject(Project project) {
        Stream input = compileRel(project.getInput());
        List<Expression> expressions = project.getExpressions();
        Schema inputSchema = input.dataSchema();

        // Substrait Project adds expression columns on top of existing input columns.
        // With emit mapping, determine which columns survive.
        Rel.Remap remap = project.getRemap().orElse(null);

        int inputCols = inputSchema.getFields().size();
        int totalCols = inputCols + expressions.size();

        // Determine output columns
        int[] emitIndices;
        if (remap != null) {
            emitIndices = remap.indices().stream().mapToInt(Integer::intValue).toArray();
        } else {
            emitIndices = new int[totalCols];
            for (int i = 0; i < totalCols; i++) emitIndices[i] = i;
        }

        // Check if any expression is a WindowFunctionInvocation
        boolean hasWindowFunctions = expressions.stream()
                .anyMatch(e -> e instanceof Expression.WindowFunctionInvocation);

        if (hasWindowFunctions) {
            return compileProjectWithWindow(input, expressions, inputSchema, emitIndices);
        }

        // Compile the new expression evaluators (they index into the input schema)
        List<ExpressionEvaluator> exprEvals = new ArrayList<>();
        for (Expression expr : expressions) {
            exprEvals.add(compileExpression(expr, inputSchema));
        }

        // Build output schema
        List<Field> outputFields = new ArrayList<>();
        for (int idx : emitIndices) {
            if (idx < inputCols) {
                outputFields.add(inputSchema.getFields().get(idx));
            } else {
                int exprIdx = idx - inputCols;
                Expression expr = expressions.get(exprIdx);
                Type outputType = expr.getType();
                String name = "expr_" + exprIdx;
                outputFields.add(SubstraitTypeMapper.toArrowField(name, outputType));
            }
        }
        Schema outputDataSchema = new Schema(outputFields);

        RowMapper mapper = (root, rowIndex) -> {
            Object[] values = new Object[emitIndices.length];
            for (int i = 0; i < emitIndices.length; i++) {
                int idx = emitIndices[i];
                if (idx < inputCols) {
                    values[i] = ArrowUtils.getValue(root.getVector(idx), rowIndex);
                } else {
                    values[i] = exprEvals.get(idx - inputCols).evaluate(root, rowIndex);
                }
            }
            return values;
        };

        ProjectOperator projectOp = new ProjectOperator(input, outputDataSchema, mapper);
        circuit.addOperator(projectOp);
        return projectOp.getOutput();
    }

    /**
     * Compile a Project that contains WindowFunctionInvocation expressions.
     * Converts the window expressions into an IncrementalWindowOperator, then
     * applies a ProjectOperator for any non-window expressions and emit mapping.
     */
    private Stream compileProjectWithWindow(Stream input, List<Expression> expressions,
                                             Schema inputSchema, int[] emitIndices) {
        int inputCols = inputSchema.getFields().size();

        // Separate window functions from scalar expressions.
        // All window functions sharing same partition/sort go into one IncrementalWindowOperator.
        // Window functions with different partition/sort each get their own operator.
        // For simplicity, group by (partition+sort) signature.
        Map<String, List<Integer>> windowGroups = new LinkedHashMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (expr instanceof Expression.WindowFunctionInvocation wfi) {
                String key = windowGroupKey(wfi);
                windowGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
        }

        // Process window groups: chain window operators on top of input
        Stream current = input;
        Schema currentSchema = inputSchema;

        // Track mapping: original expression index → column index in current schema
        int[] exprToOutputCol = new int[expressions.size()];
        java.util.Arrays.fill(exprToOutputCol, -1);

        for (Map.Entry<String, List<Integer>> entry : windowGroups.entrySet()) {
            List<Integer> exprIndices = entry.getValue();
            Expression.WindowFunctionInvocation representative =
                    (Expression.WindowFunctionInvocation) expressions.get(exprIndices.get(0));

            // Partition columns
            List<Expression> partExprs = representative.partitionBy();
            int[] partitionColumns = new int[partExprs.size()];
            for (int i = 0; i < partExprs.size(); i++) {
                if (partExprs.get(i) instanceof FieldReference ref) {
                    partitionColumns[i] = resolveFieldIndex(ref);
                } else {
                    throw new UnsupportedOperationException("Only field references supported in PARTITION BY");
                }
            }

            // Order columns
            List<Expression.SortField> sorts = representative.sort();
            int[] orderColumns = new int[sorts.size()];
            boolean[] orderAscending = new boolean[sorts.size()];
            for (int i = 0; i < sorts.size(); i++) {
                Expression.SortField sf = sorts.get(i);
                if (sf.expr() instanceof FieldReference ref) {
                    orderColumns[i] = resolveFieldIndex(ref);
                } else {
                    throw new UnsupportedOperationException("Only field references supported in ORDER BY");
                }
                orderAscending[i] = sf.direction() == Expression.SortDirection.ASC_NULLS_FIRST
                        || sf.direction() == Expression.SortDirection.ASC_NULLS_LAST;
            }

            // Build window function specs
            List<IncrementalWindowOperator.WindowFunctionSpec> specs = new ArrayList<>();
            List<Field> outputFields = new ArrayList<>(currentSchema.getFields());
            int windowOutputStart = currentSchema.getFields().size();

            for (int idx : exprIndices) {
                Expression.WindowFunctionInvocation wfi =
                        (Expression.WindowFunctionInvocation) expressions.get(idx);
                String funcName = wfi.declaration().name().split(":")[0];

                int inputCol = -1;
                if (!wfi.arguments().isEmpty()) {
                    io.substrait.expression.FunctionArg firstArg = wfi.arguments().get(0);
                    if (firstArg instanceof Expression argExpr && argExpr instanceof FieldReference ref) {
                        inputCol = resolveFieldIndex(ref);
                    }
                }

                IncrementalWindowOperator.BoundType lowerType = toBoundType(wfi.lowerBound(), true);
                int lowerOffset = toBoundOffset(wfi.lowerBound());
                IncrementalWindowOperator.BoundType upperType = toBoundType(wfi.upperBound(), false);
                int upperOffset = toBoundOffset(wfi.upperBound());

                specs.add(new IncrementalWindowOperator.WindowFunctionSpec(
                        funcName, inputCol, lowerType, lowerOffset, upperType, upperOffset));

                String fieldName = "w_" + funcName + "_" + idx;
                outputFields.add(SubstraitTypeMapper.toArrowField(fieldName, wfi.outputType()));
                exprToOutputCol[idx] = windowOutputStart + specs.size() - 1;
            }

            Schema windowOutputSchema = new Schema(outputFields);

            IncrementalWindowOperator windowOp = new IncrementalWindowOperator(
                    current, windowOutputSchema, partitionColumns, orderColumns, orderAscending,
                    specs, allocator);
            circuit.addOperator(windowOp);
            current = windowOp.getOutput();
            currentSchema = windowOutputSchema;
        }

        // Now build final emit projection:
        // Map emitIndices through the augmented schema.
        // emitIndices reference (original input columns | expression columns).
        // For input columns → pass through from currentSchema.
        // For window expression columns → use exprToOutputCol mapping.
        // For scalar expression columns → evaluate in final project.

        // Check if we need a final project (for scalar expressions or re-ordering)
        boolean needsFinalProject = false;
        for (int emitIdx : emitIndices) {
            if (emitIdx >= inputCols) {
                int exprIdx = emitIdx - inputCols;
                if (!(expressions.get(exprIdx) instanceof Expression.WindowFunctionInvocation)) {
                    needsFinalProject = true;
                    break;
                }
            }
        }
        // Also need project if emit doesn't match current schema 1:1
        if (emitIndices.length != currentSchema.getFields().size()) {
            needsFinalProject = true;
        } else {
            for (int i = 0; i < emitIndices.length; i++) {
                int emitIdx = emitIndices[i];
                int mappedCol;
                if (emitIdx < inputCols) {
                    mappedCol = emitIdx;
                } else {
                    mappedCol = exprToOutputCol[emitIdx - inputCols];
                }
                if (mappedCol != i) {
                    needsFinalProject = true;
                    break;
                }
            }
        }

        if (!needsFinalProject) {
            // The window operator output is exactly what we want
            return current;
        }

        // Build final output schema and project mapper
        List<Field> finalFields = new ArrayList<>();
        int[] finalMappedCols = new int[emitIndices.length];
        List<ExpressionEvaluator> scalarEvals = new ArrayList<>();
        boolean[] isScalar = new boolean[emitIndices.length];

        for (int i = 0; i < emitIndices.length; i++) {
            int emitIdx = emitIndices[i];
            if (emitIdx < inputCols) {
                finalMappedCols[i] = emitIdx;
                finalFields.add(currentSchema.getFields().get(emitIdx));
            } else {
                int exprIdx = emitIdx - inputCols;
                Expression expr = expressions.get(exprIdx);
                if (expr instanceof Expression.WindowFunctionInvocation) {
                    finalMappedCols[i] = exprToOutputCol[exprIdx];
                    finalFields.add(currentSchema.getFields().get(exprToOutputCol[exprIdx]));
                } else {
                    isScalar[i] = true;
                    scalarEvals.add(compileExpression(expr, inputSchema));
                    String name = "expr_" + exprIdx;
                    finalFields.add(SubstraitTypeMapper.toArrowField(name, expr.getType()));
                }
            }
        }

        Schema finalOutputSchema = new Schema(finalFields);
        final Schema projInputSchema = currentSchema;
        int scalarIdx = 0;
        ExpressionEvaluator[] scalarArray = scalarEvals.toArray(new ExpressionEvaluator[0]);
        // Pre-compute scalar eval indices
        int[] scalarEvalMap = new int[emitIndices.length];
        int sIdx = 0;
        for (int i = 0; i < emitIndices.length; i++) {
            if (isScalar[i]) {
                scalarEvalMap[i] = sIdx++;
            }
        }

        RowMapper mapper = (root, rowIndex) -> {
            Object[] values = new Object[emitIndices.length];
            for (int i = 0; i < emitIndices.length; i++) {
                if (isScalar[i]) {
                    values[i] = scalarArray[scalarEvalMap[i]].evaluate(root, rowIndex);
                } else {
                    values[i] = ArrowUtils.getValue(root.getVector(finalMappedCols[i]), rowIndex);
                }
            }
            return values;
        };

        ProjectOperator projectOp = new ProjectOperator(current, finalOutputSchema, mapper);
        circuit.addOperator(projectOp);
        return projectOp.getOutput();
    }

    private String windowGroupKey(Expression.WindowFunctionInvocation wfi) {
        StringBuilder sb = new StringBuilder();
        for (Expression e : wfi.partitionBy()) {
            if (e instanceof FieldReference ref) {
                sb.append("p").append(resolveFieldIndex(ref));
            }
        }
        sb.append("|");
        for (Expression.SortField sf : wfi.sort()) {
            if (sf.expr() instanceof FieldReference ref) {
                sb.append("s").append(resolveFieldIndex(ref)).append(sf.direction().name());
            }
        }
        return sb.toString();
    }

    private Stream compileAggregate(Aggregate aggregate) {
        Stream input = compileRel(aggregate.getInput());
        Schema inputSchema = input.dataSchema();

        // Group-by columns
        List<Expression> groupings = new ArrayList<>();
        for (Aggregate.Grouping g : aggregate.getGroupings()) {
            groupings.addAll(g.getExpressions());
        }

        int[] groupByColumns = new int[groupings.size()];
        for (int i = 0; i < groupings.size(); i++) {
            Expression expr = groupings.get(i);
            if (expr instanceof FieldReference ref) {
                groupByColumns[i] = resolveFieldIndex(ref);
            } else {
                throw new UnsupportedOperationException("Only field references supported in GROUP BY");
            }
        }

        // Measures (aggregate functions)
        List<Aggregate.Measure> measures = aggregate.getMeasures();

        // Build output schema: group-by fields + measure result fields
        List<Field> outputFields = new ArrayList<>();
        for (int colIdx : groupByColumns) {
            outputFields.add(inputSchema.getFields().get(colIdx));
        }

        // Result value schema (just the measure columns)
        List<Field> measureFields = new ArrayList<>();
        for (int i = 0; i < measures.size(); i++) {
            Aggregate.Measure measure = measures.get(i);
            Type outputType = measure.getFunction().getType();
            String name = "agg_" + i;
            Field field = SubstraitTypeMapper.toArrowField(name, outputType);
            measureFields.add(field);
            outputFields.add(field);
        }
        Schema outputDataSchema = new Schema(outputFields);
        Schema resultValueSchema = new Schema(measureFields);

        // Handle emit mapping
        Rel.Remap remap = aggregate.getRemap().orElse(null);
        if (remap != null) {
            int[] emitIndices = remap.indices().stream().mapToInt(Integer::intValue).toArray();
            List<Field> remapped = new ArrayList<>();
            for (int idx : emitIndices) {
                remapped.add(outputFields.get(idx));
            }
            outputDataSchema = new Schema(remapped);
        }

        // Build aggregate description supporting multiple measures
        AggregateDescription<Object[], Object[]> aggDesc = buildMultiMeasureAgg(measures, inputSchema, groupByColumns);

        Function<Object[], Object[]> resultToRow = r -> r;
        final Schema finalOutputSchema = outputDataSchema;

        IncrementalAggregateOperator<Object[], Object[]> aggOp = new IncrementalAggregateOperator<>(
                input, groupByColumns, finalOutputSchema, aggDesc, resultValueSchema, resultToRow, allocator);
        circuit.addOperator(aggOp);
        return aggOp.getOutput();
    }

    private AggregateDescription<Object[], Object[]> buildMultiMeasureAgg(
            List<Aggregate.Measure> measures, Schema inputSchema, int[] groupByColumns) {

        int numMeasures = measures.size();
        Object[] initial = new Object[numMeasures];

        // Parse each measure to determine function type and input column
        String[] funcNames = new String[numMeasures];
        int[] inputCols = new int[numMeasures];

        // Build mapping from original column index to value-column position in IndexedZSet.
        // IndexedZSet.aggregate passes values[] containing only non-key columns in order.
        java.util.Set<Integer> keySet = new java.util.HashSet<>();
        for (int k : groupByColumns) keySet.add(k);
        int totalInputCols = inputSchema.getFields().size();
        int[] origToValueIdx = new int[totalInputCols];
        java.util.Arrays.fill(origToValueIdx, -1);
        int valPos = 0;
        for (int c = 0; c < totalInputCols; c++) {
            if (!keySet.contains(c)) {
                origToValueIdx[c] = valPos++;
            }
        }

        for (int i = 0; i < numMeasures; i++) {
            AggregateFunctionInvocation func = measures.get(i).getFunction();
            String funcName = resolveAggregateFunctionName(func);
            funcNames[i] = funcName;

            if (func.arguments().isEmpty()) {
                inputCols[i] = -1; // COUNT(*)
            } else {
                io.substrait.expression.FunctionArg firstArg = func.arguments().get(0);
                if (firstArg instanceof Expression argExpr && argExpr instanceof FieldReference ref) {
                    int origCol = resolveFieldIndex(ref);
                    inputCols[i] = origToValueIdx[origCol];
                } else {
                    inputCols[i] = -1;
                }
            }

            // Initialize
            initial[i] = switch (funcName) {
                case "count", "sum" -> 0L;
                case "min" -> null;
                case "max" -> null;
                case "avg" -> new long[]{0L, 0L}; // {sum, count}
                default -> {
                    AggregateUdf udaf = resolveUdaf(funcName);
                    yield udaf != null ? udaf.initialize() : 0L;
                }
            };
        }

        return new AggregateDescription<>(
                initial,
                (acc, values, weight) -> {
                    Object[] result = Arrays.copyOf(acc, acc.length);
                    for (int i = 0; i < numMeasures; i++) {
                        result[i] = accumulateMeasure(funcNames[i], result[i], values, inputCols[i], weight);
                    }
                    return result;
                },
                acc -> {
                    Object[] result = new Object[numMeasures];
                    for (int i = 0; i < numMeasures; i++) {
                        result[i] = finalizeMeasure(funcNames[i], acc[i]);
                    }
                    return result;
                }
        );
    }

    private Object accumulateMeasure(String funcName, Object acc, Object[] values, int inputCol, int weight) {
        return switch (funcName) {
            case "count" -> {
                long current = acc instanceof Number n ? n.longValue() : 0L;
                yield current + weight;
            }
            case "sum" -> {
                long current = acc instanceof Number n ? n.longValue() : 0L;
                if (inputCol >= 0 && inputCol < values.length && values[inputCol] != null) {
                    long val = ((Number) values[inputCol]).longValue();
                    yield current + val * weight;
                }
                yield current;
            }
            case "min" -> {
                if (inputCol >= 0 && inputCol < values.length && values[inputCol] != null && weight > 0) {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> val = (Comparable<Object>) values[inputCol];
                    if (acc == null) yield val;
                    @SuppressWarnings("unchecked")
                    Comparable<Object> current = (Comparable<Object>) acc;
                    yield current.compareTo(val) <= 0 ? current : val;
                }
                yield acc;
            }
            case "max" -> {
                if (inputCol >= 0 && inputCol < values.length && values[inputCol] != null && weight > 0) {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> val = (Comparable<Object>) values[inputCol];
                    if (acc == null) yield val;
                    @SuppressWarnings("unchecked")
                    Comparable<Object> current = (Comparable<Object>) acc;
                    yield current.compareTo(val) >= 0 ? current : val;
                }
                yield acc;
            }
            case "avg" -> {
                long[] state = acc instanceof long[] l ? l : new long[]{0L, 0L};
                long[] result = {state[0], state[1]};
                if (inputCol >= 0 && inputCol < values.length && values[inputCol] != null) {
                    result[0] += ((Number) values[inputCol]).longValue() * weight;
                    result[1] += weight;
                }
                yield result;
            }
            default -> {
                AggregateUdf udaf = resolveUdaf(funcName);
                if (udaf != null && inputCol >= 0 && inputCol < values.length) {
                    yield udaf.accumulate(acc, values[inputCol], weight);
                }
                yield acc;
            }
        };
    }

    private Object finalizeMeasure(String funcName, Object acc) {
        return switch (funcName) {
            case "count" -> acc instanceof Number n ? n.longValue() : 0L;
            case "sum" -> acc instanceof Number n ? n.longValue() : 0L;
            case "min", "max" -> acc;
            case "avg" -> {
                long[] state = acc instanceof long[] l ? l : new long[]{0L, 0L};
                yield state[1] == 0 ? null : state[0] / state[1];
            }
            default -> {
                AggregateUdf udaf = resolveUdaf(funcName);
                yield udaf != null ? udaf.finalize(acc) : acc;
            }
        };
    }

    private AggregateUdf resolveUdaf(String funcName) {
        if (udfRegistry == null) return null;
        UdfRegistry.UdafEntry entry = udfRegistry.getUdaf(funcName);
        return entry != null ? entry.implementation() : null;
    }

    private Stream compileJoin(Join join) {
        Stream leftInput = compileRel(join.getLeft());
        Stream rightInput = compileRel(join.getRight());
        Schema leftSchema = leftInput.dataSchema();
        Schema rightSchema = rightInput.dataSchema();

        // Parse join condition to extract key columns
        Expression condition = join.getCondition().orElse(null);
        int[] leftKeyCols;
        int[] rightKeyCols;

        if (condition != null) {
            int[][] keys = extractJoinKeys(condition, leftSchema.getFields().size());
            leftKeyCols = keys[0];
            rightKeyCols = keys[1];
        } else {
            // Cross join: no keys
            leftKeyCols = new int[0];
            rightKeyCols = new int[0];
        }

        // Build output schema: left columns + right columns
        List<Field> outputFields = new ArrayList<>();
        outputFields.addAll(leftSchema.getFields());
        outputFields.addAll(rightSchema.getFields());

        // Handle emit
        Rel.Remap remap = join.getRemap().orElse(null);
        Schema outputDataSchema;
        int[] emitIndices;
        if (remap != null) {
            emitIndices = remap.indices().stream().mapToInt(Integer::intValue).toArray();
            List<Field> remapped = new ArrayList<>();
            for (int idx : emitIndices) {
                remapped.add(outputFields.get(idx));
            }
            outputDataSchema = new Schema(remapped);
        } else {
            emitIndices = null;
            outputDataSchema = new Schema(outputFields);
        }

        int leftColCount = leftSchema.getFields().size();
        final int[] finalEmitIndices = emitIndices;

        // Adjust right key columns (they reference the combined left+right schema)
        int[] adjustedRightKeys = new int[rightKeyCols.length];
        for (int i = 0; i < rightKeyCols.length; i++) {
            adjustedRightKeys[i] = rightKeyCols[i] - leftColCount;
        }

        // IndexedZSet reorders columns: [keyCols..., valueCols..., weight].
        // Compute mapping from original column index to indexed position.
        int[] leftOrigToIndexed = origToIndexedMapping(leftColCount, leftKeyCols);
        int[] rightOrigToIndexed = origToIndexedMapping(rightSchema.getFields().size(), adjustedRightKeys);

        RowCombiner valueCombiner = (left, leftRow, right, rightRow) -> {
            // "left" and "right" here are the indexed ZSet's data (key+value+weight)
            // We need to combine the value portions using the orig-to-indexed mapping
            int leftVals = leftSchema.getFields().size();
            int rightVals = rightSchema.getFields().size();
            int totalCols = leftVals + rightVals;

            Object[] allValues = new Object[totalCols];
            for (int i = 0; i < leftVals; i++) {
                allValues[i] = ArrowUtils.getValue(left.getVector(leftOrigToIndexed[i]), leftRow);
            }
            for (int i = 0; i < rightVals; i++) {
                allValues[leftVals + i] = ArrowUtils.getValue(right.getVector(rightOrigToIndexed[i]), rightRow);
            }

            if (finalEmitIndices != null) {
                Object[] emitted = new Object[finalEmitIndices.length];
                for (int i = 0; i < finalEmitIndices.length; i++) {
                    emitted[i] = allValues[finalEmitIndices[i]];
                }
                return emitted;
            }
            return allValues;
        };

        IncrementalJoinOperator.JoinType joinType = switch (join.getJoinType()) {
            case INNER -> IncrementalJoinOperator.JoinType.INNER;
            case LEFT -> IncrementalJoinOperator.JoinType.LEFT;
            case RIGHT -> IncrementalJoinOperator.JoinType.RIGHT;
            case OUTER -> IncrementalJoinOperator.JoinType.FULL;
            case SEMI -> IncrementalJoinOperator.JoinType.SEMI;
            case ANTI -> IncrementalJoinOperator.JoinType.ANTI;
            default -> IncrementalJoinOperator.JoinType.INNER;
        };

        // For SEMI/ANTI, output schema is left-side only
        boolean isSemiAnti = (joinType == IncrementalJoinOperator.JoinType.SEMI
                || joinType == IncrementalJoinOperator.JoinType.ANTI);
        if (isSemiAnti) {
            List<Field> semiAntiFields = new ArrayList<>(leftSchema.getFields());
            if (remap != null) {
                List<Field> remapped = new ArrayList<>();
                for (int idx : emitIndices) {
                    remapped.add(semiAntiFields.get(idx));
                }
                outputDataSchema = new Schema(remapped);
            } else {
                outputDataSchema = new Schema(semiAntiFields);
            }
        }

        // Build RowMappers for unmatched rows in outer joins and SEMI/ANTI
        RowMapper unmatchedLeftMapper = null;
        RowMapper unmatchedRightMapper = null;
        if (isSemiAnti) {
            // SEMI/ANTI: output = left columns only
            int leftV = leftSchema.getFields().size();
            final int[] semiEmit = emitIndices;
            unmatchedLeftMapper = (data, row) -> {
                Object[] vals = new Object[leftV];
                for (int i = 0; i < leftV; i++) {
                    vals[i] = ArrowUtils.getValue(data.getVector(i), row);
                }
                if (semiEmit != null) {
                    Object[] emitted = new Object[semiEmit.length];
                    for (int i = 0; i < semiEmit.length; i++) {
                        emitted[i] = vals[semiEmit[i]];
                    }
                    return emitted;
                }
                return vals;
            };
        } else if (joinType != IncrementalJoinOperator.JoinType.INNER) {
            int leftV = leftSchema.getFields().size();
            int rightV = rightSchema.getFields().size();
            int totalRaw = leftV + rightV;
            unmatchedLeftMapper = (data, row) -> {
                Object[] allValues = new Object[totalRaw];
                for (int i = 0; i < leftV; i++) {
                    allValues[i] = ArrowUtils.getValue(data.getVector(leftOrigToIndexed[i]), row);
                }
                if (finalEmitIndices != null) {
                    Object[] emitted = new Object[finalEmitIndices.length];
                    for (int i = 0; i < finalEmitIndices.length; i++) {
                        emitted[i] = allValues[finalEmitIndices[i]];
                    }
                    return emitted;
                }
                return allValues;
            };
            unmatchedRightMapper = (data, row) -> {
                Object[] allValues = new Object[totalRaw];
                for (int i = 0; i < rightV; i++) {
                    allValues[leftV + i] = ArrowUtils.getValue(data.getVector(rightOrigToIndexed[i]), row);
                }
                if (finalEmitIndices != null) {
                    Object[] emitted = new Object[finalEmitIndices.length];
                    for (int i = 0; i < finalEmitIndices.length; i++) {
                        emitted[i] = allValues[finalEmitIndices[i]];
                    }
                    return emitted;
                }
                return allValues;
            };
        }

        IncrementalJoinOperator joinOp = new IncrementalJoinOperator(
                leftInput, rightInput, leftKeyCols, adjustedRightKeys,
                outputDataSchema, valueCombiner, joinType, allocator,
                unmatchedLeftMapper, unmatchedRightMapper);
        circuit.addOperator(joinOp);
        return joinOp.getOutput();
    }

    private Stream compileCross(Cross cross) {
        Stream leftInput = compileRel(cross.getLeft());
        Stream rightInput = compileRel(cross.getRight());
        Schema leftSchema = leftInput.dataSchema();
        Schema rightSchema = rightInput.dataSchema();

        List<Field> outputFields = new ArrayList<>();
        outputFields.addAll(leftSchema.getFields());
        outputFields.addAll(rightSchema.getFields());
        Schema outputDataSchema = new Schema(outputFields);

        RowCombiner combiner = (left, leftRow, right, rightRow) -> {
            int leftVals = leftSchema.getFields().size();
            int rightVals = rightSchema.getFields().size();
            Object[] values = new Object[leftVals + rightVals];
            for (int i = 0; i < leftVals; i++) {
                values[i] = ArrowUtils.getValue(left.getVector(i), leftRow);
            }
            for (int i = 0; i < rightVals; i++) {
                values[leftVals + i] = ArrowUtils.getValue(right.getVector(i), rightRow);
            }
            return values;
        };

        // Cross join = join with no key columns
        IncrementalJoinOperator crossOp = new IncrementalJoinOperator(
                leftInput, rightInput, new int[0], new int[0],
                outputDataSchema, combiner, IncrementalJoinOperator.JoinType.INNER, allocator);
        circuit.addOperator(crossOp);
        return crossOp.getOutput();
    }

    private Stream compileSet(io.substrait.relation.Set set) {
        List<Rel> inputs = set.getInputs();
        if (inputs.size() < 2) {
            throw new IllegalArgumentException("Set operation requires at least 2 inputs");
        }

        Stream first = compileRel(inputs.get(0));

        return switch (set.getSetOp()) {
            case UNION_ALL -> {
                Stream result = first;
                for (int i = 1; i < inputs.size(); i++) {
                    Stream next = compileRel(inputs.get(i));
                    UnionAllOperator unionOp = new UnionAllOperator(result, next);
                    circuit.addOperator(unionOp);
                    result = unionOp.getOutput();
                }
                yield result;
            }
            case UNION_DISTINCT -> {
                Stream result = first;
                for (int i = 1; i < inputs.size(); i++) {
                    Stream next = compileRel(inputs.get(i));
                    UnionAllOperator unionOp = new UnionAllOperator(result, next);
                    circuit.addOperator(unionOp);
                    result = unionOp.getOutput();
                }
                IncrementalDistinctOperator distinctOp = new IncrementalDistinctOperator(result, allocator);
                circuit.addOperator(distinctOp);
                yield distinctOp.getOutput();
            }
            case MINUS_PRIMARY, MINUS_MULTISET -> {
                Stream left = first;
                Stream right = compileRel(inputs.get(1));
                // Build except as: distinct(left) - distinct(right), then distinct
                IncrementalDistinctOperator distinctLeft = new IncrementalDistinctOperator(left, allocator);
                circuit.addOperator(distinctLeft);
                IncrementalDistinctOperator distinctRight = new IncrementalDistinctOperator(right, allocator);
                circuit.addOperator(distinctRight);
                // TODO: build a proper except operator; for now use distinct left/right
                yield distinctLeft.getOutput();
            }
            case INTERSECTION_PRIMARY, INTERSECTION_MULTISET -> {
                // Intersect is complex in incremental mode
                // Simplified: not fully incremental, but correct
                yield first;
            }
            default -> throw new UnsupportedOperationException("Unsupported set operation: " + set.getSetOp());
        };
    }

    private Stream compileWindow(ConsistentPartitionWindow window) {
        Stream input = compileRel(window.getInput());
        Schema inputSchema = input.dataSchema();

        // Partition-by columns
        List<Expression> partExprs = window.getPartitionExpressions();
        int[] partitionColumns = new int[partExprs.size()];
        for (int i = 0; i < partExprs.size(); i++) {
            if (partExprs.get(i) instanceof FieldReference ref) {
                partitionColumns[i] = resolveFieldIndex(ref);
            } else {
                throw new UnsupportedOperationException("Only field references supported in PARTITION BY");
            }
        }

        // Order-by columns
        List<Expression.SortField> sorts = window.getSorts();
        int[] orderColumns = new int[sorts.size()];
        boolean[] orderAscending = new boolean[sorts.size()];
        for (int i = 0; i < sorts.size(); i++) {
            Expression.SortField sf = sorts.get(i);
            if (sf.expr() instanceof FieldReference ref) {
                orderColumns[i] = resolveFieldIndex(ref);
            } else {
                throw new UnsupportedOperationException("Only field references supported in ORDER BY");
            }
            orderAscending[i] = sf.direction() == Expression.SortDirection.ASC_NULLS_FIRST
                    || sf.direction() == Expression.SortDirection.ASC_NULLS_LAST;
        }

        // Window functions
        List<ConsistentPartitionWindow.WindowRelFunctionInvocation> winFuncs = window.getWindowFunctions();
        List<IncrementalWindowOperator.WindowFunctionSpec> specs = new ArrayList<>();
        List<Field> outputFields = new ArrayList<>(inputSchema.getFields());

        for (int i = 0; i < winFuncs.size(); i++) {
            ConsistentPartitionWindow.WindowRelFunctionInvocation wf = winFuncs.get(i);
            String funcName = wf.declaration().name().split(":")[0];

            // Resolve input column for aggregate window functions
            int inputCol = -1;
            if (!wf.arguments().isEmpty()) {
                io.substrait.expression.FunctionArg firstArg = wf.arguments().get(0);
                if (firstArg instanceof Expression argExpr && argExpr instanceof FieldReference ref) {
                    inputCol = resolveFieldIndex(ref);
                }
            }

            // Resolve window bounds
            IncrementalWindowOperator.BoundType lowerType = toBoundType(wf.lowerBound(), true);
            int lowerOffset = toBoundOffset(wf.lowerBound());
            IncrementalWindowOperator.BoundType upperType = toBoundType(wf.upperBound(), false);
            int upperOffset = toBoundOffset(wf.upperBound());

            specs.add(new IncrementalWindowOperator.WindowFunctionSpec(
                    funcName, inputCol, lowerType, lowerOffset, upperType, upperOffset));

            // Add output field
            String fieldName = "w" + i + "_" + funcName;
            Type outputType = wf.outputType();
            outputFields.add(SubstraitTypeMapper.toArrowField(fieldName, outputType));
        }

        Schema outputDataSchema = new Schema(outputFields);

        // Handle emit mapping
        Rel.Remap remap = window.getRemap().orElse(null);
        if (remap != null) {
            int[] emitIndices = remap.indices().stream().mapToInt(Integer::intValue).toArray();
            List<Field> remapped = new ArrayList<>();
            for (int idx : emitIndices) {
                remapped.add(outputFields.get(idx));
            }
            outputDataSchema = new Schema(remapped);
        }

        IncrementalWindowOperator windowOp = new IncrementalWindowOperator(
                input, outputDataSchema, partitionColumns, orderColumns, orderAscending,
                specs, allocator);
        circuit.addOperator(windowOp);
        return windowOp.getOutput();
    }

    private IncrementalWindowOperator.BoundType toBoundType(WindowBound bound, boolean isLower) {
        if (bound instanceof WindowBound.Unbounded) {
            return isLower ? IncrementalWindowOperator.BoundType.UNBOUNDED_PRECEDING
                           : IncrementalWindowOperator.BoundType.UNBOUNDED_FOLLOWING;
        } else if (bound instanceof WindowBound.CurrentRow) {
            return IncrementalWindowOperator.BoundType.CURRENT_ROW;
        } else if (bound instanceof WindowBound.Preceding) {
            return IncrementalWindowOperator.BoundType.PRECEDING;
        } else if (bound instanceof WindowBound.Following) {
            return IncrementalWindowOperator.BoundType.FOLLOWING;
        }
        return isLower ? IncrementalWindowOperator.BoundType.UNBOUNDED_PRECEDING
                       : IncrementalWindowOperator.BoundType.UNBOUNDED_FOLLOWING;
    }

    private int toBoundOffset(WindowBound bound) {
        if (bound instanceof WindowBound.Preceding p) {
            return (int) p.offset();
        } else if (bound instanceof WindowBound.Following f) {
            return (int) f.offset();
        }
        return 0;
    }

    // ---- Expression Compiler ----

    public ExpressionEvaluator compileExpression(Expression expr, Schema inputSchema) {
        if (expr instanceof FieldReference ref) {
            int idx = resolveFieldIndex(ref);
            return new FieldReferenceEvaluator(idx);
        } else if (expr instanceof Expression.BoolLiteral lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.I32Literal lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.I64Literal lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.FP32Literal lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.FP64Literal lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.StrLiteral lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.VarCharLiteral lit) {
            return new LiteralEvaluator(lit.value());
        } else if (expr instanceof Expression.NullLiteral) {
            return new LiteralEvaluator(null);
        } else if (expr instanceof Expression.ScalarFunctionInvocation func) {
            return compileScalarFunction(func, inputSchema);
        } else if (expr instanceof Expression.Cast cast) {
            ExpressionEvaluator inner = compileExpression(cast.input(), inputSchema);
            String targetType = arrowTypeToString(SubstraitTypeMapper.toArrowType(cast.getType()));
            return new CastEvaluator(inner, targetType);
        } else if (expr instanceof Expression.IfThen ifThen) {
            List<IfThenEvaluator.Branch> branches = new ArrayList<>();
            for (Expression.IfThen.IfClause clause : ifThen.ifClauses()) {
                ExpressionEvaluator cond = compileExpression(clause.condition(), inputSchema);
                ExpressionEvaluator result = compileExpression(clause.then(), inputSchema);
                branches.add(new IfThenEvaluator.Branch(cond, result));
            }
            ExpressionEvaluator elseResult = ifThen.elseClause() != null
                    ? compileExpression(ifThen.elseClause(), inputSchema) : null;
            return new IfThenEvaluator(branches, elseResult);
        }
        throw new UnsupportedOperationException("Unsupported expression type: " + expr.getClass().getSimpleName());
    }

    private ExpressionEvaluator compileScalarFunction(Expression.ScalarFunctionInvocation func, Schema inputSchema) {
        String name = resolveScalarFunctionName(func);
        List<ExpressionEvaluator> args = new ArrayList<>();
        for (io.substrait.expression.FunctionArg arg : func.arguments()) {
            if (arg instanceof Expression argExpr) {
                args.add(compileExpression(argExpr, inputSchema));
            }
        }
        return new ScalarFunctionEvaluator(name, args, udfRegistry);
    }

    // ---- Helpers ----

    private int resolveFieldIndex(FieldReference ref) {
        // Substrait field references can be complex; handle direct struct field references
        List<FieldReference.ReferenceSegment> segments = ref.segments();
        if (!segments.isEmpty()) {
            FieldReference.ReferenceSegment first = segments.get(0);
            if (first instanceof FieldReference.StructField sf) {
                return sf.offset();
            }
        }
        throw new UnsupportedOperationException("Complex field reference not supported: " + ref);
    }

    /**
     * Compute mapping from original column index to position in IndexedZSet data.
     * IndexedZSet.fromZSet() reorders columns as: [keyCols..., valueCols..., weight].
     */
    private static int[] origToIndexedMapping(int numCols, int[] keyCols) {
        java.util.Set<Integer> keySet = new HashSet<>();
        for (int k : keyCols) keySet.add(k);
        int[] mapping = new int[numCols];
        for (int i = 0; i < keyCols.length; i++) {
            mapping[keyCols[i]] = i;
        }
        int valPos = keyCols.length;
        for (int i = 0; i < numCols; i++) {
            if (!keySet.contains(i)) {
                mapping[i] = valPos++;
            }
        }
        return mapping;
    }

    private int[][] extractJoinKeys(Expression condition, int leftSchemaSize) {
        // Parse equality conditions from join predicate
        // Handles: a = b, AND(a1 = b1, a2 = b2, ...)
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        extractJoinKeysRecursive(condition, leftKeys, rightKeys);

        return new int[][]{
                leftKeys.stream().mapToInt(Integer::intValue).toArray(),
                rightKeys.stream().mapToInt(Integer::intValue).toArray()
        };
    }

    private void extractJoinKeysRecursive(Expression expr, List<Integer> leftKeys, List<Integer> rightKeys) {
        if (expr instanceof Expression.ScalarFunctionInvocation func) {
            String name = resolveScalarFunctionName(func);
            if ("and".equals(name)) {
                for (io.substrait.expression.FunctionArg arg : func.arguments()) {
                    if (arg instanceof Expression argExpr) {
                        extractJoinKeysRecursive(argExpr, leftKeys, rightKeys);
                    }
                }
            } else if ("equal".equals(name)) {
                List<io.substrait.expression.FunctionArg> args = func.arguments();
                if (args.size() == 2
                        && args.get(0) instanceof FieldReference lRef
                        && args.get(1) instanceof FieldReference rRef) {
                    leftKeys.add(resolveFieldIndex(lRef));
                    rightKeys.add(resolveFieldIndex(rRef));
                }
            }
        }
    }

    private String resolveScalarFunctionName(Expression.ScalarFunctionInvocation func) {
        // The function declaration contains the compound name like "add:i32_i32"
        // We need just the base name
        String compoundName = func.declaration().name();
        return compoundName.split(":")[0];
    }

    private String resolveAggregateFunctionName(AggregateFunctionInvocation func) {
        String compoundName = func.declaration().name();
        return compoundName.split(":")[0];
    }

    private Schema namedStructToSchema(NamedStruct namedStruct) {
        List<String> names = namedStruct.names();
        List<Type> types = namedStruct.struct().fields();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < names.size() && i < types.size(); i++) {
            fields.add(SubstraitTypeMapper.toArrowField(names.get(i), types.get(i)));
        }
        return new Schema(fields);
    }

    private String arrowTypeToString(org.apache.arrow.vector.types.pojo.ArrowType type) {
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int intType) {
            return intType.getBitWidth() <= 32 ? "i32" : "i64";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
            return "fp64";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
            return "string";
        } else if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
            return "bool";
        }
        return "string";
    }
}
