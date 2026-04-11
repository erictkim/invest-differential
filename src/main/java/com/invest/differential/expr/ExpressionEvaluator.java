package com.invest.differential.expr;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Evaluates an expression against a row of Arrow data.
 * Row-at-a-time evaluation for v1.
 */
@FunctionalInterface
public interface ExpressionEvaluator {
    Object evaluate(VectorSchemaRoot root, int rowIndex);
}
