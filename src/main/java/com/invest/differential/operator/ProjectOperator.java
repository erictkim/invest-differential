package com.invest.differential.operator;

import com.invest.differential.parallel.ParallelConfig;
import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/**
 * Project operator (SELECT expressions). Linear — inherently incremental.
 * Maps each delta row to output columns; preserves weights.
 * Supports intra-operator data parallelism when configured.
 */
public final class ProjectOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final Schema outputDataSchema;
    private final RowMapper mapper;
    private ParallelConfig parallelConfig = ParallelConfig.disabled();

    public ProjectOperator(Stream input, Schema outputDataSchema, RowMapper mapper) {
        this.input = input;
        this.output = new Stream(outputDataSchema);
        this.outputDataSchema = outputDataSchema;
        this.mapper = mapper;
    }

    @Override
    public void setParallelConfig(ParallelConfig config) {
        this.parallelConfig = config != null ? config : ParallelConfig.disabled();
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        if (!parallelConfig.isEnabled() || delta.rowCount() < parallelConfig.getMinRowsForDataParallel()) {
            output.setValue(delta.map(outputDataSchema, mapper));
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
                    ZSet mapped = part.map(outputDataSchema, mapper);
                    part.close();
                    return mapped;
                }
            }));
        }

        ZSet[] results = new ZSet[tasks.size()];
        for (int i = 0; i < tasks.size(); i++) {
            results[i] = tasks.get(i).join();
        }
        ZSet merged = ZSet.concat(results, outputDataSchema, delta.allocator());
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
    public String name() { return "Project"; }
}
