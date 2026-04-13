package com.invest.differential.parallel;

/**
 * Per-operator adaptive metrics that track an exponential moving average (EMA)
 * of execution cost and data volume. Used to decide at runtime whether an
 * operator should execute sequentially or in parallel.
 */
public final class AdaptiveMetrics {

    private static final double ALPHA = 0.3; // EMA smoothing factor

    private double emaNanos = 0;
    private double emaRows = 0;
    private long stepCount = 0;

    /**
     * Record one step's execution cost and output size.
     */
    public void recordStep(long durationNanos, long rows) {
        stepCount++;
        if (stepCount == 1) {
            // First observation: seed the EMA directly
            emaNanos = durationNanos;
            emaRows = rows;
        } else {
            emaNanos = ALPHA * durationNanos + (1 - ALPHA) * emaNanos;
            emaRows = ALPHA * rows + (1 - ALPHA) * emaRows;
        }
    }

    /**
     * Decide whether this operator should use parallel execution based on
     * observed cost and data volume.
     */
    public boolean shouldParallelize(long minNanos, int minRows) {
        return stepCount >= 1 && emaNanos > minNanos && emaRows > minRows;
    }

    /**
     * Recommend a parallelism level based on observed data volume.
     */
    public int recommendedParallelism(int maxCores) {
        if (emaRows < 500) return 1;
        if (emaRows < 5_000) return Math.min(2, maxCores);
        if (emaRows < 50_000) return Math.min(4, maxCores);
        return maxCores;
    }

    public double getEmaNanos() { return emaNanos; }
    public double getEmaRows() { return emaRows; }
    public long getStepCount() { return stepCount; }

    public void reset() {
        emaNanos = 0;
        emaRows = 0;
        stepCount = 0;
    }
}
