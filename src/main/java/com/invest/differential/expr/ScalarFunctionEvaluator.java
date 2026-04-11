package com.invest.differential.expr;

import com.invest.differential.udf.ScalarUdf;
import com.invest.differential.udf.UdfRegistry;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Evaluates scalar functions: arithmetic, comparison, boolean, string operations, and UDFs.
 */
public final class ScalarFunctionEvaluator implements ExpressionEvaluator {

    private final String functionName;
    private final List<ExpressionEvaluator> arguments;
    private final UdfRegistry udfRegistry;

    public ScalarFunctionEvaluator(String functionName, List<ExpressionEvaluator> arguments) {
        this(functionName, arguments, null);
    }

    public ScalarFunctionEvaluator(String functionName, List<ExpressionEvaluator> arguments, UdfRegistry udfRegistry) {
        this.functionName = functionName;
        this.arguments = arguments;
        this.udfRegistry = udfRegistry;
    }

    @Override
    public Object evaluate(VectorSchemaRoot root, int rowIndex) {
        return switch (functionName) {
            // Arithmetic
            case "add" -> arith(root, rowIndex, '+');
            case "subtract" -> arith(root, rowIndex, '-');
            case "multiply" -> arith(root, rowIndex, '*');
            case "divide" -> arith(root, rowIndex, '/');
            case "modulus" -> arith(root, rowIndex, '%');
            case "negate" -> negate(root, rowIndex);

            // Comparison
            case "equal" -> compare(root, rowIndex, 0);
            case "not_equal" -> !((boolean) compare(root, rowIndex, 0));
            case "lt" -> compare(root, rowIndex, -1);
            case "lte" -> compareLte(root, rowIndex);
            case "gt" -> compare(root, rowIndex, 1);
            case "gte" -> compareGte(root, rowIndex);

            // Boolean
            case "and" -> boolAnd(root, rowIndex);
            case "or" -> boolOr(root, rowIndex);
            case "not" -> boolNot(root, rowIndex);

            // Null handling
            case "is_null" -> arguments.get(0).evaluate(root, rowIndex) == null;
            case "is_not_null" -> arguments.get(0).evaluate(root, rowIndex) != null;
            case "coalesce" -> coalesce(root, rowIndex);

            // String
            case "concat" -> concat(root, rowIndex);

            default -> evaluateUdf(root, rowIndex);
        };
    }

    public String functionName() { return functionName; }
    public List<ExpressionEvaluator> arguments() { return arguments; }

    private Object evaluateUdf(VectorSchemaRoot root, int rowIndex) {
        if (udfRegistry != null) {
            UdfRegistry.UdfEntry entry = udfRegistry.get(functionName);
            if (entry != null) {
                Object[] args = new Object[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    args[i] = arguments.get(i).evaluate(root, rowIndex);
                }
                return entry.implementation().evaluate(args);
            }
        }
        throw new UnsupportedOperationException("Unknown function: " + functionName);
    }

    @SuppressWarnings("unchecked")
    private Object arith(VectorSchemaRoot root, int rowIndex, char op) {
        Object left = arguments.get(0).evaluate(root, rowIndex);
        Object right = arguments.get(1).evaluate(root, rowIndex);
        if (left == null || right == null) return null;

        if (left instanceof Integer l && right instanceof Integer r) {
            return switch (op) {
                case '+' -> Math.addExact(l, r);
                case '-' -> Math.subtractExact(l, r);
                case '*' -> Math.multiplyExact(l, r);
                case '/' -> r == 0 ? null : l / r;
                case '%' -> r == 0 ? null : l % r;
                default -> throw new IllegalArgumentException();
            };
        } else if (left instanceof Long l && right instanceof Long r) {
            return switch (op) {
                case '+' -> Math.addExact(l, r);
                case '-' -> Math.subtractExact(l, r);
                case '*' -> Math.multiplyExact(l, r);
                case '/' -> r == 0 ? null : l / r;
                case '%' -> r == 0 ? null : l % r;
                default -> throw new IllegalArgumentException();
            };
        } else {
            double l = ((Number) left).doubleValue();
            double r = ((Number) right).doubleValue();
            return switch (op) {
                case '+' -> l + r;
                case '-' -> l - r;
                case '*' -> l * r;
                case '/' -> r == 0 ? null : l / r;
                case '%' -> r == 0 ? null : l % r;
                default -> throw new IllegalArgumentException();
            };
        }
    }

    private Object negate(VectorSchemaRoot root, int rowIndex) {
        Object val = arguments.get(0).evaluate(root, rowIndex);
        if (val == null) return null;
        if (val instanceof Integer i) return Math.negateExact(i);
        if (val instanceof Long l) return Math.negateExact(l);
        return -((Number) val).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private boolean compare(VectorSchemaRoot root, int rowIndex, int expected) {
        Object left = arguments.get(0).evaluate(root, rowIndex);
        Object right = arguments.get(1).evaluate(root, rowIndex);
        if (left == null || right == null) return false;
        int cmp = ((Comparable<Object>) toComparable(left)).compareTo(toComparable(right));
        return expected == 0 ? cmp == 0 : (expected < 0 ? cmp < 0 : cmp > 0);
    }

    private boolean compareLte(VectorSchemaRoot root, int rowIndex) {
        Object left = arguments.get(0).evaluate(root, rowIndex);
        Object right = arguments.get(1).evaluate(root, rowIndex);
        if (left == null || right == null) return false;
        @SuppressWarnings("unchecked")
        int cmp = ((Comparable<Object>) toComparable(left)).compareTo(toComparable(right));
        return cmp <= 0;
    }

    private boolean compareGte(VectorSchemaRoot root, int rowIndex) {
        Object left = arguments.get(0).evaluate(root, rowIndex);
        Object right = arguments.get(1).evaluate(root, rowIndex);
        if (left == null || right == null) return false;
        @SuppressWarnings("unchecked")
        int cmp = ((Comparable<Object>) toComparable(left)).compareTo(toComparable(right));
        return cmp >= 0;
    }

    @SuppressWarnings("rawtypes")
    private Comparable toComparable(Object val) {
        if (val instanceof Comparable c) return c;
        return val.toString();
    }

    private boolean boolAnd(VectorSchemaRoot root, int rowIndex) {
        for (ExpressionEvaluator arg : arguments) {
            Object val = arg.evaluate(root, rowIndex);
            if (val == null || !((boolean) val)) return false;
        }
        return true;
    }

    private boolean boolOr(VectorSchemaRoot root, int rowIndex) {
        for (ExpressionEvaluator arg : arguments) {
            Object val = arg.evaluate(root, rowIndex);
            if (val != null && (boolean) val) return true;
        }
        return false;
    }

    private boolean boolNot(VectorSchemaRoot root, int rowIndex) {
        Object val = arguments.get(0).evaluate(root, rowIndex);
        if (val == null) return false;
        return !((boolean) val);
    }

    private Object coalesce(VectorSchemaRoot root, int rowIndex) {
        for (ExpressionEvaluator arg : arguments) {
            Object val = arg.evaluate(root, rowIndex);
            if (val != null) return val;
        }
        return null;
    }

    private Object concat(VectorSchemaRoot root, int rowIndex) {
        StringBuilder sb = new StringBuilder();
        for (ExpressionEvaluator arg : arguments) {
            Object val = arg.evaluate(root, rowIndex);
            if (val == null) return null;
            sb.append(val);
        }
        return sb.toString();
    }
}
