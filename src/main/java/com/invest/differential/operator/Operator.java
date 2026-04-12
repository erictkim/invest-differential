package com.invest.differential.operator;

/**
 * Base interface for all operators in the dataflow graph.
 */
public interface Operator {
    /** Execute one step: compute output from current inputs. */
    void step();

    /** Reset all internal state (keeps operator ready for reuse). */
    void reset();

    /** Release all resources. After close(), the operator cannot be used. */
    default void close() { reset(); }

    /** The output stream of this operator. */
    Stream getOutput();

    /** Name for debugging. */
    String name();

    /** Per-operator metrics. Returns null if metrics are not tracked. */
    default OperatorMetrics getMetrics() { return null; }
}
