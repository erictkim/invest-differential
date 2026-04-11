package com.invest.differential.operator;

import com.invest.differential.zset.RowMapper;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Project operator (SELECT expressions). Linear — inherently incremental.
 * Maps each delta row to output columns; preserves weights.
 */
public final class ProjectOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final Schema outputDataSchema;
    private final RowMapper mapper;

    public ProjectOperator(Stream input, Schema outputDataSchema, RowMapper mapper) {
        this.input = input;
        this.output = new Stream(outputDataSchema);
        this.outputDataSchema = outputDataSchema;
        this.mapper = mapper;
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        output.setValue(delta.map(outputDataSchema, mapper));
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
