package com.invest.differential.udf;

/**
 * A user-defined aggregate function (UDAF).
 *
 * <p>Implementations define a fold pattern: initialize an accumulator,
 * accumulate values with weights, and finalize the result.
 *
 * <p>The weight parameter supports incremental view maintenance where
 * rows can be inserted (weight &gt; 0) or retracted (weight &lt; 0).
 */
public interface AggregateUdf {

    /**
     * @return the initial accumulator value for a fresh group
     */
    Object initialize();

    /**
     * Accumulate a single row's value into the current state.
     *
     * @param accumulator current accumulator state
     * @param value       the column value for this row (may be null)
     * @param weight      the weight of the row (positive for insert, negative for retract)
     * @return updated accumulator state
     */
    Object accumulate(Object accumulator, Object value, int weight);

    /**
     * Produce the final result from the accumulator.
     *
     * @param accumulator final accumulator state after all rows are processed
     * @return the aggregate result
     */
    Object finalize(Object accumulator);
}
