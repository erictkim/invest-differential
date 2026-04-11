package com.invest.differential.zset;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Combines two rows (from two different VectorSchemaRoots) into output column values.
 */
@FunctionalInterface
public interface RowCombiner {
    /**
     * @param left       left VectorSchemaRoot
     * @param leftRow    row index in left
     * @param right      right VectorSchemaRoot
     * @param rightRow   row index in right
     * @return an Object[] of output column values (one per output schema column, excluding weight)
     */
    Object[] combine(VectorSchemaRoot left, int leftRow, VectorSchemaRoot right, int rightRow);
}
