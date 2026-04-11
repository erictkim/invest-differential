package com.invest.differential.expr;

import com.invest.differential.arrow.ArrowUtils;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Reads a column value at a given field index.
 */
public final class FieldReferenceEvaluator implements ExpressionEvaluator {

    private final int fieldIndex;

    public FieldReferenceEvaluator(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public Object evaluate(VectorSchemaRoot root, int rowIndex) {
        return ArrowUtils.getValue(root.getVector(fieldIndex), rowIndex);
    }

    public int fieldIndex() { return fieldIndex; }
}
