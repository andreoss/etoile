/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import java.io.Serializable;

/**
 * Comparison of two values.
 * Marked Serializable for Spark.
 *
 * @since 0.7.0
 */
public interface Comparison extends Serializable {
    /**
     * Make a comparison.
     * @param right First value.
     * @param left Second value
     * @return Outcome of comparison.
     */
    Outcome make(Object right, Object left);
}

