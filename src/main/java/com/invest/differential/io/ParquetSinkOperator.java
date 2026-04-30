package com.invest.differential.io;

import com.invest.differential.operator.Operator;
import com.invest.differential.operator.Stream;
import com.invest.differential.zset.ZSet;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Operator that observes a stream's delta on each step and forwards it to a
 * {@link ParquetSerializer}. Closing this operator (via {@link com.invest.differential.operator.Circuit#close()})
 * flushes any still-live rows to the Parquet file.
 *
 * <p>The {@link #input} field is named so that {@code Circuit}'s
 * dependency-discovery reflection picks it up as an input edge, ensuring this
 * operator runs after the upstream operator that produces the delta.
 */
public final class ParquetSinkOperator implements Operator {

    private final String name;
    @SuppressWarnings("unused") // discovered via reflection by Circuit
    private final Stream input;
    private final Stream output; // unused but required by Operator API
    private final ParquetSerializer serializer;
    private boolean closed;

    public ParquetSinkOperator(String name, Stream input, ParquetSerializer serializer) {
        this.name = name;
        this.input = input;
        this.output = new Stream(input.dataSchema());
        this.serializer = serializer;
    }

    @Override
    public void step() {
        ZSet val = input.getValue();
        if (val != null) {
            serializer.recordDelta(val);
        }
    }

    @Override
    public void reset() {
        // Parquet output is append-only; reset is a no-op so prior writes are preserved.
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            serializer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream getOutput() { return output; }

    @Override
    public String name() { return "ParquetSink(" + name + ")"; }

    public ParquetSerializer getSerializer() { return serializer; }
}
