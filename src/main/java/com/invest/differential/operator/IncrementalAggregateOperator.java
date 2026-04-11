package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.arrow.RowHasher;
import com.invest.differential.zset.AggregateDescription;
import com.invest.differential.zset.IndexedZSet;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Incremental aggregate operator (GROUP BY + aggregate functions).
 *
 * Maintains the full accumulated input. On each step:
 * 1. Identifies which group keys were affected by ΔInput
 * 2. Adds ΔInput to accumulated state
 * 3. Recomputes aggregates only for affected groups
 * 4. Diffs affected groups with previous output to produce ΔOutput
 *
 * Unaffected groups are skipped entirely, making updates proportional
 * to the number of changed groups rather than the total number of groups.
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
    private final int[] outputKeyColumns; // group-by key positions in output schema (0..K-1)

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
        this.outputKeyColumns = new int[groupByColumns.length];
        for (int i = 0; i < groupByColumns.length; i++) outputKeyColumns[i] = i;
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();

        // Fast path: no changes → empty output delta
        if (delta.isEmpty()) {
            output.setValue(ZSet.empty(outputDataSchema, allocator));
            return;
        }

        // 1. Identify affected group keys from the delta
        Set<Integer> affectedKeyHashes = new HashSet<>();
        for (int row = 0; row < delta.rawRowCount(); row++) {
            affectedKeyHashes.add(RowHasher.hashRow(delta.data(), row, groupByColumns));
        }

        // 2. Accumulate: state += Δ
        ZSet newState = accumulatedInput.add(delta);
        newState.compact();
        accumulatedInput.close();
        accumulatedInput = newState;

        // 3. Compute aggregate output for affected groups only
        ZSet newAffectedOutput;
        if (accumulatedInput.isEmpty()) {
            newAffectedOutput = ZSet.empty(outputDataSchema, allocator);
        } else {
            IndexedZSet indexed = accumulatedInput.index(groupByColumns);
            IndexedZSet partialAgg = indexed.aggregateForKeys(
                    affectedKeyHashes, aggDescription, resultValueSchema, resultToRow);

            newAffectedOutput = partialAgg.flatten(outputDataSchema, (root, rowIndex) -> {
                int totalCols = outputDataSchema.getFields().size();
                Object[] values = new Object[totalCols];
                for (int i = 0; i < totalCols; i++) {
                    values[i] = ArrowUtils.getValue(root.getVector(i), rowIndex);
                }
                return values;
            });
            newAffectedOutput.compact();
            indexed.close();
            partialAgg.close();
        }

        // 4. Extract old output for affected keys from previousOutput
        ZSet oldAffectedOutput = previousOutput.filter((root, rowIndex) -> {
            int hash = RowHasher.hashRow(root, rowIndex, outputKeyColumns);
            return affectedKeyHashes.contains(hash);
        });

        // 5. Delta = new affected output - old affected output
        ZSet deltaOutput = newAffectedOutput.subtract(oldAffectedOutput);
        deltaOutput.compact();

        // 6. Update previousOutput by applying the delta
        //    (retracts old values and adds new values for affected keys)
        ZSet updatedOutput = previousOutput.add(deltaOutput);
        updatedOutput.compact();
        previousOutput.close();
        previousOutput = updatedOutput;

        newAffectedOutput.close();
        oldAffectedOutput.close();

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
    public void close() {
        if (accumulatedInput != null) { accumulatedInput.close(); accumulatedInput = null; }
        if (previousOutput != null) { previousOutput.close(); previousOutput = null; }
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "IncrementalAggregate"; }
}
