package com.invest.differential.expr;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Returns a constant literal value.
 */
public final class LiteralEvaluator implements ExpressionEvaluator {

    private final Object value;

    public LiteralEvaluator(Object value) {
        this.value = value;
    }

    @Override
    public Object evaluate(VectorSchemaRoot root, int rowIndex) {
        return value;
    }
}
