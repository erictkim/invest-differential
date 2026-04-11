package com.invest.differential.expr;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Type cast evaluator.
 */
public final class CastEvaluator implements ExpressionEvaluator {

    private final ExpressionEvaluator input;
    private final String targetType; // "i32", "i64", "fp64", "string", "bool"

    public CastEvaluator(ExpressionEvaluator input, String targetType) {
        this.input = input;
        this.targetType = targetType;
    }

    @Override
    public Object evaluate(VectorSchemaRoot root, int rowIndex) {
        Object val = input.evaluate(root, rowIndex);
        if (val == null) return null;

        return switch (targetType) {
            case "i32", "int" -> {
                if (val instanceof Number n) yield n.intValue();
                yield Integer.parseInt(val.toString());
            }
            case "i64", "long" -> {
                if (val instanceof Number n) yield n.longValue();
                yield Long.parseLong(val.toString());
            }
            case "fp32", "float" -> {
                if (val instanceof Number n) yield n.floatValue();
                yield Float.parseFloat(val.toString());
            }
            case "fp64", "double" -> {
                if (val instanceof Number n) yield n.doubleValue();
                yield Double.parseDouble(val.toString());
            }
            case "string", "varchar" -> val.toString();
            case "bool", "boolean" -> {
                if (val instanceof Boolean b) yield b;
                if (val instanceof Number n) yield n.intValue() != 0;
                yield Boolean.parseBoolean(val.toString());
            }
            default -> throw new UnsupportedOperationException("Unsupported cast target: " + targetType);
        };
    }
}
