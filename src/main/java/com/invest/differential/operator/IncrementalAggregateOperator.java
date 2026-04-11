package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.AggregateDescription;
import com.invest.differential.zset.IndexedZSet;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.function.Function;

/**
 * Incremental aggregate operator (GROUP BY + aggregate functions).
 *
 * Maintains the full accumulated input. On each step:
 * 1. Adds ΔInput to accumulated state
 * 2. Recomputes the full aggregate output
 * 3. Diffs with previous aggregate output to produce ΔOutput
 *
 * This is correct but not optimal for large unchanging groups.
 * Future optimization: track which groups were affected by ΔInput and only recompute those.
 */
public final class IncrementalAggregateOperator<R, IR> implements Operator {

    private final Stream input;
    private final Stream output;
    private final BufferAllocator allocator;
    private final int[] groupByColumns;
    private final Schema outputDataSchema;
    private final AggregateDescription<R, IR> aggDescription;
    private final Schema resultValueSchema;
    private final Function<R, Object[]> resultToRow;

    private ZSet accumulatedInput;  // full input (sum of all deltas)
    private ZSet previousOutput;    // previous aggregate output (full)

    public IncrementalAggregateOperator(Stream input, int[] groupByColumns,
                                         Schema outputDataSchema,
                                         AggregateDescription<R, IR> aggDescription,
                                         Schema resultValueSchema,
                                         Function<R, Object[]> resultToRow,
                                         BufferAllocator allocator) {
        this.input = input;
        this.output = new Stream(outputDataSchema);
        this.allocator = allocator;
        this.groupByColumns = groupByColumns;
        this.outputDataSchema = outputDataSchema;
        this.aggDescription = aggDescription;
        this.resultValueSchema = resultValueSchema;
        this.resultToRow = resultToRow;
        this.accumulatedInput = ZSet.empty(input.dataSchema(), allocator);
        this.previousOutput = ZSet.empty(outputDataSchema, allocator);
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();

        // Accumulate: state += Δ
        ZSet newState = accumulatedInput.add(delta);
        newState.compact();
        accumulatedInput.close();
        accumulatedInput = newState;

        // Compute full aggregate output
        ZSet fullOutput;
        if (accumulatedInput.isEmpty()) {
            fullOutput = ZSet.empty(outputDataSchema, allocator);
        } else {
            IndexedZSet indexed = accumulatedInput.index(groupByColumns);
            IndexedZSet aggregated = indexed.aggregate(aggDescription, resultValueSchema, resultToRow);

            // Flatten: combine key + aggregate result into output row
            fullOutput = aggregated.flatten(outputDataSchema, (root, rowIndex) -> {
                int totalCols = outputDataSchema.getFields().size();
                Object[] values = new Object[totalCols];
                for (int i = 0; i < totalCols; i++) {
                    values[i] = ArrowUtils.getValue(root.getVector(i), rowIndex);
                }
                return values;
            });
            fullOutput.compact();
            indexed.close();
            aggregated.close();
        }

        // Diff: ΔOutput = fullOutput - previousOutput
        ZSet deltaOutput = fullOutput.subtract(previousOutput);
        deltaOutput.compact();
        previousOutput.close();
        previousOutput = fullOutput;

        output.setValue(deltaOutput);
    }

    @Override
    public void reset() {
        if (accumulatedInput != null) accumulatedInput.close();
        if (previousOutput != null) previousOutput.close();
        accumulatedInput = ZSet.empty(input.dataSchema(), allocator);
        previousOutput = ZSet.empty(outputDataSchema, allocator);
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalAggregate"; }
}
