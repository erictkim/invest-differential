package com.invest.differential.zset;

import java.util.function.Function;

/**
 * Describes a weight-aware aggregation fold over ZSet entries.
 *
 * @param <R>    result type
 * @param <IR>   intermediate result type
 */
public final class AggregateDescription<R, IR> {

    private final IR initialValue;
    private final AggregateAccumulator<IR> accumulator;
    private final Function<IR, R> finalizer;

    public AggregateDescription(IR initialValue, AggregateAccumulator<IR> accumulator, Function<IR, R> finalizer) {
        this.initialValue = initialValue;
        this.accumulator = accumulator;
        this.finalizer = finalizer;
    }

    public IR initialValue() { return initialValue; }
    public AggregateAccumulator<IR> accumulator() { return accumulator; }
    public Function<IR, R> finalizer() { return finalizer; }

    @FunctionalInterface
    public interface AggregateAccumulator<IR> {
        /**
         * @param current    current intermediate result
         * @param values     column values for the current row (Object[])
         * @param weight     the weight of the current row
         * @return updated intermediate result
         */
        IR accumulate(IR current, Object[] values, int weight);
    }
}
