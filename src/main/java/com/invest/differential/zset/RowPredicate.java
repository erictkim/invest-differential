package com.invest.differential.zset;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Predicate evaluated on a row of an Arrow VectorSchemaRoot.
 */
@FunctionalInterface
public interface RowPredicate {
    boolean test(VectorSchemaRoot root, int rowIndex);
}
