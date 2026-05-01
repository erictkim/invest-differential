package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.udf.TableUdf;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Table-valued UDF operator: invokes a {@link TableUdf} once per input row,
 * emitting zero or more output rows for each input row. Input row weight is
 * propagated to all emitted rows (so retractions cancel correctly).
 *
 * <p>Linear and stateless — naturally incremental.
 */
public final class TableUdfOperator implements Operator {

    private final Stream input;
    private final Stream output;
    private final Schema outputDataSchema;
    private final TableUdf udf;
    private final int[] argColumnIndices;

    public TableUdfOperator(Stream input,
                            Schema outputDataSchema,
                            TableUdf udf,
                            int[] argColumnIndices) {
        this.input = input;
        this.outputDataSchema = outputDataSchema;
        this.output = new Stream(outputDataSchema);
        this.udf = udf;
        this.argColumnIndices = argColumnIndices.clone();
    }

    @Override
    public void step() {
        ZSet delta = input.getValue();
        Schema outFull = ArrowUtils.createSchemaWithWeight(outputDataSchema);
        VectorSchemaRoot result = VectorSchemaRoot.create(outFull, delta.allocator());
        result.allocateNew();
        int outWeightCol = result.getFieldVectors().size() - 1;
        int inWeightCol = delta.data().getFieldVectors().size() - 1;
        int outRow = 0;
        int outCols = outputDataSchema.getFields().size();

        for (int row = 0; row < delta.data().getRowCount(); row++) {
            int weight = ((IntVector) delta.data().getVector(inWeightCol)).get(row);
            Object[] args = new Object[argColumnIndices.length];
            for (int i = 0; i < argColumnIndices.length; i++) {
                args[i] = ArrowUtils.getValue(delta.data().getVector(argColumnIndices[i]), row);
            }
            Iterable<Object[]> emitted = udf.evaluate(args);
            if (emitted == null) continue;
            for (Object[] outValues : emitted) {
                if (outValues.length != outCols) {
                    throw new IllegalStateException(
                            "TableUdf returned row with " + outValues.length
                                    + " columns; expected " + outCols);
                }
                for (int col = 0; col < outCols; col++) {
                    ArrowUtils.setValue(result.getVector(col), outRow, outValues[col]);
                }
                ((IntVector) result.getVector(outWeightCol)).setSafe(outRow, weight);
                outRow++;
            }
        }
        result.setRowCount(outRow);
        output.setValue(ZSet.fromRoot(outputDataSchema, result, delta.allocator()));
    }

    @Override
    public void reset() {
        output.clear();
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "TableUdf"; }
}
