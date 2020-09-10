/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;

/**
 * Transformation of {@link Dataset}.
 *
 * @param <Y> Underlying data type.
 * @since 0.1.0
 */
interface Transformation<Y> {
    /**
     * Convert it back to dataset.
     *
     * @return The Spark dataset
     */
    Dataset<Y> get();

}
