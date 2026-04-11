package com.invest.differential.expr;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Conditional expression: IF/THEN/ELSE (CASE WHEN).
 */
public final class IfThenEvaluator implements ExpressionEvaluator {

    public record Branch(ExpressionEvaluator condition, ExpressionEvaluator result) {}

    private final List<Branch> branches;
    private final ExpressionEvaluator elseResult; // nullable

    public IfThenEvaluator(List<Branch> branches, ExpressionEvaluator elseResult) {
        this.branches = branches;
        this.elseResult = elseResult;
    }

    @Override
    public Object evaluate(VectorSchemaRoot root, int rowIndex) {
        for (Branch branch : branches) {
            Object cond = branch.condition().evaluate(root, rowIndex);
            if (cond instanceof Boolean b && b) {
                return branch.result().evaluate(root, rowIndex);
            }
        }
        if (elseResult != null) {
            return elseResult.evaluate(root, rowIndex);
        }
        return null;
    }
}
