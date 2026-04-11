package com.invest.differential.zset;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Maps an input row to output column values.
 */
@FunctionalInterface
public interface RowMapper {
    /**
     * @param root     the source VectorSchemaRoot
     * @param rowIndex the row index in root
     * @return an Object[] of output column values (one per output schema column, excluding weight)
     */
    Object[] map(VectorSchemaRoot root, int rowIndex);
}
