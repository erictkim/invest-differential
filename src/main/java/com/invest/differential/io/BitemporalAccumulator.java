package com.invest.differential.io;

/**
 * One bitemporal observation of a row collected by an
 * {@link AccumulatorSinkOperator}: the row's column values plus a
 * {@code startTime} (when the row was created) and {@code endTime} (when the
 * row was retracted, or the engine was closed without a matching retraction).
 *
 * <p>The order and types of {@link #values} match the Arrow {@link
 * org.apache.arrow.vector.types.pojo.Schema} returned by
 * {@link AccumulatorSinkOperator#schema()}.
 */
public record BitemporalAccumulator(Object[] values, long startTime, long endTime) {
}
