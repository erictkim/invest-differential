package com.invest.differential.operator;

import java.util.*;

/**
 * A circuit is a dataflow graph of operators executed in topological order.
 */
public final class Circuit {

    private final List<Operator> operators = new ArrayList<>();
    private final List<InputOperator> inputs = new ArrayList<>();
    private final List<OutputOperator> outputs = new ArrayList<>();
    private final Map<Operator, OperatorMetrics> metricsMap = new IdentityHashMap<>();
    private boolean metricsEnabled = false;

    public void addOperator(Operator op) {
        operators.add(op);
        metricsMap.put(op, new OperatorMetrics());
        if (op instanceof InputOperator inp) {
            inputs.add(inp);
        } else if (op instanceof OutputOperator out) {
            outputs.add(out);
        }
    }

    public void setMetricsEnabled(boolean enabled) {
        this.metricsEnabled = enabled;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Execute one step: run all operators in topological order.
     */
    public void step() {
        for (Operator op : operators) {
            if (metricsEnabled) {
                long start = System.nanoTime();
                op.step();
                long elapsed = System.nanoTime() - start;
                long rows = 0;
                if (op.getOutput() != null && op.getOutput().getValue() != null) {
                    rows = op.getOutput().getValue().rowCount();
                }
                metricsMap.get(op).recordStep(elapsed, rows);
            } else {
                op.step();
            }
        }
    }

    /**
     * Get metrics for all operators.
     */
    public Map<String, OperatorMetrics> getMetrics() {
        Map<String, OperatorMetrics> result = new LinkedHashMap<>();
        for (int i = 0; i < operators.size(); i++) {
            result.put(i + ":" + operators.get(i).name(), metricsMap.get(operators.get(i)));
        }
        return result;
    }

    /**
     * Get metrics for a specific operator by index.
     */
    public OperatorMetrics getMetrics(int index) {
        return metricsMap.get(operators.get(index));
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

    /**
     * Export the operator dataflow graph as a DOT (Graphviz) string.
     */
    public String toDot() {
        // Build map: Stream identity → producing operator index
        Map<Stream, Integer> streamSource = new IdentityHashMap<>();
        for (int i = 0; i < operators.size(); i++) {
            streamSource.put(operators.get(i).getOutput(), i);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("digraph Circuit {\n");
        sb.append("  rankdir=TB;\n");
        sb.append("  node [shape=box, style=filled, fillcolor=lightblue];\n\n");

        // Node labels
        for (int i = 0; i < operators.size(); i++) {
            Operator op = operators.get(i);
            String label = op.name();
            sb.append("  op").append(i).append(" [label=\"").append(label).append("\"];\n");
        }

        sb.append("\n");

        // Edges: for each operator, find its input streams via reflection
        for (int i = 0; i < operators.size(); i++) {
            Operator op = operators.get(i);
            List<Stream> inputStreams = getInputStreams(op);
            for (Stream in : inputStreams) {
                Integer sourceIdx = streamSource.get(in);
                if (sourceIdx != null) {
                    sb.append("  op").append(sourceIdx).append(" -> op").append(i).append(";\n");
                }
            }
        }

        sb.append("}\n");
        return sb.toString();
    }

    private List<Stream> getInputStreams(Operator op) {
        List<Stream> streams = new ArrayList<>();
        for (java.lang.reflect.Field field : op.getClass().getDeclaredFields()) {
            if (field.getType() == Stream.class && !field.getName().equals("output")) {
                field.setAccessible(true);
                try {
                    Stream s = (Stream) field.get(op);
                    if (s != null) streams.add(s);
                } catch (IllegalAccessException e) {
                    // skip
                }
            }
        }
        return streams;
    }
}
