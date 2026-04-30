package com.invest.differential.io;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.operator.Operator;
import com.invest.differential.operator.Stream;
import com.invest.differential.zset.ZSet;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Operator that observes a stream's delta on each step and accumulates one
 * {@link BitemporalAccumulator} record per row that is created (positive
 * weight) and either later retracted (matching negative weight) or still live
 * when the engine/circuit closes.
 *
 * <p>Each unit of positive weight is tracked separately (FIFO) so a row that
 * has been inserted N times produces N bitemporal observations.
 *
 * <p>The accumulated rows are retrievable via {@link #rows()} (snapshot
 * including currently-live rows, with {@code endTime}=current clock) or
 * {@link #closedRows()} (only rows whose deletion has been observed). The
 * Arrow {@link Schema} associated with each row's {@code values} array is
 * available via {@link #schema()}; column names and types align positionally.
 *
 * <p>The {@link #input} field is named so that {@code Circuit}'s
 * dependency-discovery reflection picks it up as an input edge, ensuring this
 * operator runs after the upstream operator that produces the delta.
 */
public final class AccumulatorSinkOperator implements Operator {

    private final String name;
    @SuppressWarnings("unused") // discovered via reflection by Circuit
    private final Stream input;
    private final Stream output; // unused but required by Operator API
    private final Schema dataSchema;
    private final List<Field> dataFields;
    private final LongSupplier clock;
    private final Map<RowKey, Deque<Long>> live = new HashMap<>();
    private final List<BitemporalAccumulator> closedRows = new ArrayList<>();
    private boolean closed;

    public AccumulatorSinkOperator(String name, Stream input, Schema dataSchema, LongSupplier clock) {
        this.name = name;
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.dataSchema = dataSchema;
        this.dataFields = dataSchema.getFields();
        this.clock = clock;
    }

    /** The Arrow schema describing the layout of each row's {@code values} array. */
    public Schema schema() {
        return dataSchema;
    }

    /**
     * Process one delta directly (not via the operator step loop). Insertions
     * (positive weights) start tracking live rows; deletions (negative weights)
     * match against live rows and emit a closed {@link BitemporalAccumulator}.
     */
    public void recordDelta(ZSet delta) {
        if (closed) {
            throw new IllegalStateException("AccumulatorSinkOperator is closed");
        }
        if (delta == null) return;
        VectorSchemaRoot root = delta.data();
        int rowCount = root.getRowCount();
        if (rowCount == 0) return;

        int weightIdx = ArrowUtils.getWeightColumnIndex(root);
        IntVector weightVec = (IntVector) root.getVector(weightIdx);
        long now = clock.getAsLong();

        for (int row = 0; row < rowCount; row++) {
            int weight = weightVec.get(row);
            if (weight == 0) continue;
            Object[] values = readRow(root, row);
            RowKey key = new RowKey(values);
            if (weight > 0) {
                Deque<Long> q = live.computeIfAbsent(key, k -> new ArrayDeque<>());
                for (int i = 0; i < weight; i++) {
                    q.addLast(now);
                }
            } else {
                int retract = -weight;
                Deque<Long> q = live.get(key);
                for (int i = 0; i < retract; i++) {
                    Long startTime = (q != null) ? q.pollFirst() : null;
                    if (q != null && q.isEmpty()) {
                        live.remove(key);
                    }
                    closedRows.add(new BitemporalAccumulator(
                            values, startTime != null ? startTime : now, now));
                }
            }
        }
    }

    /**
     * Snapshot of all observed rows, including currently-live rows whose
     * {@code endTime} is the current clock value (they have not yet been
     * retracted). The operator remains open and can keep recording deltas.
     */
    public List<BitemporalAccumulator> rows() {
        List<BitemporalAccumulator> all = new ArrayList<>(closedRows);
        long now = clock.getAsLong();
        for (Map.Entry<RowKey, Deque<Long>> e : live.entrySet()) {
            Object[] values = e.getKey().values;
            for (Long start : e.getValue()) {
                all.add(new BitemporalAccumulator(values, start, now));
            }
        }
        return all;
    }

    /**
     * Only rows for which a matching retraction has been observed (or that
     * were flushed by {@link #close()}). Excludes rows that are still live in
     * an open operator.
     */
    public List<BitemporalAccumulator> closedRows() {
        return Collections.unmodifiableList(closedRows);
    }

    @Override
    public void step() {
        ZSet val = input.getValue();
        if (val != null) {
            recordDelta(val);
        }
    }

    @Override
    public void reset() {
        // Accumulated rows are append-only; reset is a no-op so prior records are preserved.
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        long endTs = clock.getAsLong();
        for (Iterator<Map.Entry<RowKey, Deque<Long>>> it = live.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<RowKey, Deque<Long>> e = it.next();
            Object[] values = e.getKey().values;
            for (Long start : e.getValue()) {
                closedRows.add(new BitemporalAccumulator(values, start, endTs));
            }
            it.remove();
        }
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "AccumulatorSink(" + name + ")"; }

    // ---- internals ----

    private Object[] readRow(VectorSchemaRoot root, int rowIndex) {
        Object[] values = new Object[dataFields.size()];
        for (int i = 0; i < values.length; i++) {
            FieldVector v = root.getVector(i);
            if (v.isNull(rowIndex)) {
                values[i] = null;
            } else {
                values[i] = ArrowUtils.getValue(v, rowIndex);
            }
        }
        return values;
    }
}
