package com.invest.differential.operator;

/**
 * Base interface for all operators in the dataflow graph.
 */
public interface Operator {
    /** Execute one step: compute output from current inputs. */
    void step();

    /** Reset all internal state. */
    void reset();

    /** The output stream of this operator. */
    Stream getOutput();

    /** Name for debugging. */
    String name();
}
