package com.invest.differential.parallel;

import java.util.concurrent.ForkJoinPool;

/**
 * Configuration for parallel execution of the incremental circuit.
 *
 * <p>Controls whether operators execute in parallel, the degree of data parallelism
 * within operators, and adaptive thresholds for switching between sequential and
 * parallel modes at runtime.
 */
public final class ParallelConfig {

    /** Singleton disabled config — all execution is sequential. */
    private static final ParallelConfig DISABLED = new ParallelConfig(false, 1, 0, 0, null);

    private final boolean enabled;
    private final int maxParallelism;
    private final int minRowsForDataParallel;
    private final long minNanosForAdaptive;
    private final ForkJoinPool pool;

    private ParallelConfig(boolean enabled, int maxParallelism,
                           int minRowsForDataParallel, long minNanosForAdaptive,
                           ForkJoinPool pool) {
        this.enabled = enabled;
        this.maxParallelism = maxParallelism;
        this.minRowsForDataParallel = minRowsForDataParallel;
        this.minNanosForAdaptive = minNanosForAdaptive;
        this.pool = pool;
    }

    /**
     * Create a disabled config — all operators run sequentially.
     */
    public static ParallelConfig disabled() {
        return DISABLED;
    }

    /**
     * Create a parallel config using available processors.
     * Uses default thresholds: 500 rows minimum for data parallelism,
     * 100µs minimum operator cost for adaptive switching.
     */
    public static ParallelConfig withDefaults() {
        int cores = Runtime.getRuntime().availableProcessors();
        return new ParallelConfig(
                true,
                cores,
                500,
                100_000L, // 100µs
                new ForkJoinPool(cores)
        );
    }

    /**
     * Create a parallel config with specific parallelism.
     */
    public static ParallelConfig withParallelism(int parallelism) {
        return new ParallelConfig(
                true,
                parallelism,
                500,
                100_000L,
                new ForkJoinPool(parallelism)
        );
    }

    /**
     * Create a fully customized parallel config.
     */
    public static ParallelConfig custom(int maxParallelism, int minRowsForDataParallel,
                                         long minNanosForAdaptive) {
        return new ParallelConfig(
                true,
                maxParallelism,
                minRowsForDataParallel,
                minNanosForAdaptive,
                new ForkJoinPool(maxParallelism)
        );
    }

    public boolean isEnabled() { return enabled; }
    public int getMaxParallelism() { return maxParallelism; }
    public int getMinRowsForDataParallel() { return minRowsForDataParallel; }
    public long getMinNanosForAdaptive() { return minNanosForAdaptive; }
    public ForkJoinPool getPool() { return pool; }

    /**
     * Shut down the thread pool. Call when engine is closed.
     */
    public void shutdown() {
        if (pool != null && !pool.isShutdown()) {
            pool.shutdown();
        }
    }
}
