package com.invest.differential.udf;

/**
 * A user-defined scalar function.
 *
 * <p>Implementations receive evaluated argument values and return a result.
 * Null arguments are passed through; implementations should handle nulls explicitly.
 */
@FunctionalInterface
public interface ScalarUdf {
    Object evaluate(Object... args);
}
