package com.invest.differential.operator;

import com.invest.differential.parallel.AdaptiveMetrics;
import com.invest.differential.parallel.ParallelConfig;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * A circuit is a dataflow graph of operators executed in topological order.
 */
public final class Circuit {

    private final List<Operator> operators = new ArrayList<>();
    private final List<InputOperator> inputs = new ArrayList<>();
    private final List<OutputOperator> outputs = new ArrayList<>();
    private final Map<Operator, OperatorMetrics> metricsMap = new IdentityHashMap<>();
    private final Map<Operator, AdaptiveMetrics> adaptiveMetricsMap = new IdentityHashMap<>();
    private boolean metricsEnabled = false;
    private ParallelConfig parallelConfig = ParallelConfig.disabled();
    private List<List<Operator>> waves; // cached wavefront schedule

    public void addOperator(Operator op) {
        operators.add(op);
        metricsMap.put(op, new OperatorMetrics());
        adaptiveMetricsMap.put(op, new AdaptiveMetrics());
        waves = null; // invalidate cached wavefront
        if (op instanceof InputOperator inp) {
            inputs.add(inp);
        } else if (op instanceof OutputOperator out) {
            outputs.add(out);
        }
    }

    public void setParallelConfig(ParallelConfig config) {
        this.parallelConfig = config != null ? config : ParallelConfig.disabled();
        waves = null; // invalidate to rebuild
        // Propagate to all operators for intra-operator data parallelism
        for (Operator op : operators) {
            op.setParallelConfig(this.parallelConfig);
        }
    }

    public ParallelConfig getParallelConfig() {
        return parallelConfig;
    }

    public void setMetricsEnabled(boolean enabled) {
        this.metricsEnabled = enabled;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Execute one step: run all operators in topological order.
     * If parallel config is enabled, independent operators in the same wave
     * execute concurrently.
     */
    public void step() {
        if (parallelConfig.isEnabled()) {
            stepParallel();
        } else {
            stepSequential();
        }
    }

    private void stepSequential() {
        for (Operator op : operators) {
            long start = metricsEnabled ? System.nanoTime() : 0;
            op.step();
            if (metricsEnabled) {
                long elapsed = System.nanoTime() - start;
                long rows = 0;
                if (op.getOutput() != null && op.getOutput().getValue() != null) {
                    rows = op.getOutput().getValue().rowCount();
                }
                metricsMap.get(op).recordStep(elapsed, rows);
                adaptiveMetricsMap.get(op).recordStep(elapsed, rows);
            }
        }
    }

    private void stepParallel() {
        if (waves == null) {
            waves = buildWaves();
        }
        for (List<Operator> wave : waves) {
            if (wave.size() == 1) {
                // Single operator — no threading overhead
                Operator op = wave.get(0);
                long start = metricsEnabled ? System.nanoTime() : 0;
                op.step();
                if (metricsEnabled) {
                    long elapsed = System.nanoTime() - start;
                    long rows = 0;
                    if (op.getOutput() != null && op.getOutput().getValue() != null) {
                        rows = op.getOutput().getValue().rowCount();
                    }
                    metricsMap.get(op).recordStep(elapsed, rows);
                    adaptiveMetricsMap.get(op).recordStep(elapsed, rows);
                }
            } else {
                // Multiple independent operators — run in parallel
                List<ForkJoinTask<?>> tasks = new ArrayList<>(wave.size());
                for (Operator op : wave) {
                    tasks.add(parallelConfig.getPool().submit(new RecursiveAction() {
                        @Override
                        protected void compute() {
                            long start = metricsEnabled ? System.nanoTime() : 0;
                            op.step();
                            if (metricsEnabled) {
                                long elapsed = System.nanoTime() - start;
                                long rows = 0;
                                if (op.getOutput() != null && op.getOutput().getValue() != null) {
                                    rows = op.getOutput().getValue().rowCount();
                                }
                                metricsMap.get(op).recordStep(elapsed, rows);
                                adaptiveMetricsMap.get(op).recordStep(elapsed, rows);
                            }
                        }
                    }));
                }
                // Wait for all operators in this wave to complete
                for (ForkJoinTask<?> task : tasks) {
                    task.join();
                }
            }
        }
    }

    /**
     * Build wavefront schedule: group operators into waves where all operators
     * in a wave have no dependencies on each other (only on earlier waves).
     */
    public List<List<Operator>> buildWaves() {
        // Build dependency graph: op -> set of ops it depends on
        Map<Stream, Operator> streamProducer = new IdentityHashMap<>();
        for (Operator op : operators) {
            if (op.getOutput() != null) {
                streamProducer.put(op.getOutput(), op);
            }
        }

        Map<Operator, Set<Operator>> deps = new IdentityHashMap<>();
        for (Operator op : operators) {
            Set<Operator> opDeps = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Stream inputStream : getInputStreams(op)) {
                Operator producer = streamProducer.get(inputStream);
                if (producer != null) {
                    opDeps.add(producer);
                }
            }
            deps.put(op, opDeps);
        }

        // Assign depth (wave index) to each operator
        Map<Operator, Integer> depth = new IdentityHashMap<>();
        for (Operator op : operators) {
            computeDepth(op, deps, depth);
        }

        // Group by depth
        int maxDepth = 0;
        for (int d : depth.values()) maxDepth = Math.max(maxDepth, d);

        List<List<Operator>> result = new ArrayList<>();
        for (int i = 0; i <= maxDepth; i++) {
            result.add(new ArrayList<>());
        }
        for (Operator op : operators) {
            result.get(depth.get(op)).add(op);
        }
        return result;
    }

    private int computeDepth(Operator op, Map<Operator, Set<Operator>> deps,
                              Map<Operator, Integer> depth) {
        if (depth.containsKey(op)) return depth.get(op);
        int maxDep = -1;
        for (Operator dep : deps.get(op)) {
            maxDep = Math.max(maxDep, computeDepth(dep, deps, depth));
        }
        int d = maxDep + 1;
        depth.put(op, d);
        return d;
    }

    /**
     * Get the adaptive metrics for a specific operator (for data-parallel decisions).
     */
    public AdaptiveMetrics getAdaptiveMetrics(Operator op) {
        return adaptiveMetricsMap.get(op);
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
