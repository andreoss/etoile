/*
 * Copyright(C) 2019. See COPYING for more.
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

    /**
     * A @{link Transformation} which does nothing.
     *
     * @param <Y> Underlying type
     * @since 0.1.0
     */
    final class Noop<Y> extends TransformationEnvelope<Y> {
        /**
         * Ctor.
         *
         * @param tran A transformation
         */
        Noop(final Transformation<Y> tran) {
            super(() -> tran);
        }

        /**
         * Ctor.
         *
         * @param set A dataset
         */
        Noop(final Dataset<Y> set) {
            super(() -> set);
        }
    }

}
