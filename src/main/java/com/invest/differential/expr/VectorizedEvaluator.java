package com.invest.differential.expr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Evaluates an expression over an entire Arrow batch at once,
 * producing a result FieldVector rather than one boxed Object per row.
 * Caller owns the returned vector and must close it.
 */
@FunctionalInterface
public interface VectorizedEvaluator {
    FieldVector evaluate(VectorSchemaRoot batch, BufferAllocator allocator);
}
