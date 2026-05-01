package com.invest.differential.udf;

/**
 * A user-defined table function (UDTF): one input row may produce zero or more
 * output rows, each with a fixed multi-column schema.
 *
 * <p>Unlike {@link ScalarUdf} (which produces a single value per input row),
 * a {@code TableUdf} is invoked once per input row and may return any number
 * of output rows. The returned arrays must match the column order and Arrow
 * types of the output schema declared at registration time.
 *
 * <p>Implementations must be deterministic and side-effect free for correct
 * incremental view maintenance: when an input row is retracted (negative
 * weight), the operator re-invokes the function with the same arguments and
 * emits the produced rows with negated weights.
 */
@FunctionalInterface
public interface TableUdf {
    /**
     * Produce zero or more rows from the supplied argument values.
     *
     * @param args evaluated argument values from the source row, in the order
     *             specified by the {@code argColumnIndices} at registration
     * @return an iterable of rows; each row's element order/types must match
     *         the declared output schema. May be empty; never {@code null}.
     */
    Iterable<Object[]> evaluate(Object... args);
}
