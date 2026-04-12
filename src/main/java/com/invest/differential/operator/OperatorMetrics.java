package com.invest.differential.operator;

/**
 * Tracks per-operator metrics: step count, processing time, and rows processed.
 */
public final class OperatorMetrics {

    private long stepCount;
    private long totalNanos;
    private long rowsProduced;

    public void recordStep(long durationNanos, long rows) {
        stepCount++;
        totalNanos += durationNanos;
        rowsProduced += rows;
    }

    public long getStepCount() { return stepCount; }
    public long getTotalNanos() { return totalNanos; }
    public double getTotalMillis() { return totalNanos / 1_000_000.0; }
    public long getRowsProduced() { return rowsProduced; }

    public double getAvgStepMillis() {
        return stepCount == 0 ? 0 : getTotalMillis() / stepCount;
    }

    public void reset() {
        stepCount = 0;
        totalNanos = 0;
        rowsProduced = 0;
    }

    @Override
    public String toString() {
        return String.format("steps=%d, totalMs=%.2f, avgMs=%.2f, rows=%d",
                stepCount, getTotalMillis(), getAvgStepMillis(), rowsProduced);
    }
}
