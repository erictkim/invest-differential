package com.invest.differential.operator;

import java.util.ArrayList;
import java.util.List;

/**
 * A circuit is a dataflow graph of operators executed in topological order.
 */
public final class Circuit {

    private final List<Operator> operators = new ArrayList<>();
    private final List<InputOperator> inputs = new ArrayList<>();
    private final List<OutputOperator> outputs = new ArrayList<>();

    public void addOperator(Operator op) {
        operators.add(op);
        if (op instanceof InputOperator inp) {
            inputs.add(inp);
        } else if (op instanceof OutputOperator out) {
            outputs.add(out);
        }
    }

    /**
     * Execute one step: run all operators in topological order.
     */
    public void step() {
        for (Operator op : operators) {
            op.step();
        }
    }

    /**
     * Reset all operator state.
     */
    public void reset() {
        for (Operator op : operators) {
            op.reset();
        }
    }

    public List<InputOperator> getInputs() { return inputs; }
    public List<OutputOperator> getOutputs() { return outputs; }
    public List<Operator> getOperators() { return operators; }

    public InputOperator getInput(String tableName) {
        for (InputOperator inp : inputs) {
            if (inp.tableName().equalsIgnoreCase(tableName)) {
                return inp;
            }
        }
        throw new IllegalArgumentException("No input operator for table: " + tableName);
    }

    /**
     * Find an existing InputOperator by table name, or null if not found.
     */
    public InputOperator findInput(String tableName) {
        for (InputOperator inp : inputs) {
            if (inp.tableName().equalsIgnoreCase(tableName)) {
                return inp;
            }
        }
        return null;
    }

    /**
     * Close all operator state and release Arrow resources.
     */
    public void close() {
        for (Operator op : operators) {
            op.close();
        }
    }
}
