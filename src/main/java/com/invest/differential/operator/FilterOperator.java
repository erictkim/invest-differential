package com.invest.differential.operator;

import com.invest.differential.parallel.ParallelConfig;
import com.invest.differential.zset.RowPredicate;
import com.invest.differential.zset.ZSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/**
 * Filter operator (WHERE clause). Linear — inherently incremental.
 * Applies predicate to each delta row; preserves weights.
 * Supports intra-operator data parallelism when configured.
 */
public final class FilterOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final RowPredicate predicate;
    private ParallelConfig parallelConfig = ParallelConfig.disabled();

    public FilterOperator(Stream input, RowPredicate predicate) {
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.predicate = predicate;
    }

    @Override
    public void setParallelConfig(ParallelConfig config) {
        this.parallelConfig = config != null ? config : ParallelConfig.disabled();
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        if (!parallelConfig.isEnabled() || delta.rowCount() < parallelConfig.getMinRowsForDataParallel()) {
            output.setValue(delta.filter(predicate));
        } else {
            stepParallel(delta);
        }
    }

    private void stepParallel(ZSet delta) {
        int[] dataCols = new int[delta.dataSchema().getFields().size()];
        for (int i = 0; i < dataCols.length; i++) dataCols[i] = i;

        int parallelism = Math.min(parallelConfig.getMaxParallelism(),
                Math.max(1, delta.rowCount() / parallelConfig.getMinRowsForDataParallel()));

        ZSet[] parts = delta.hashPartition(dataCols, parallelism);

        List<ForkJoinTask<ZSet>> tasks = new ArrayList<>(parallelism);
        for (ZSet part : parts) {
            tasks.add(parallelConfig.getPool().submit(new RecursiveTask<ZSet>() {
                @Override
                protected ZSet compute() {
                    ZSet filtered = part.filter(predicate);
                    part.close();
                    return filtered;
                }
            }));
        }

        ZSet[] results = new ZSet[tasks.size()];
        for (int i = 0; i < tasks.size(); i++) {
            results[i] = tasks.get(i).join();
        }
        ZSet merged = ZSet.concat(results, delta.dataSchema(), delta.allocator());
        for (ZSet r : results) r.close();
        output.setValue(merged);
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "Filter"; }
}
