/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;

/**
 * A @{link Transformation} which does nothing.
 *
 * @param <Y> Underlying type
 * @since 0.1.0
 */
public final class Noop<Y> extends TransformationEnvelope<Y> {
    /**
     * Ctor.
     *
     * @param tran A transformation
     */
    Noop(final Transformation<Y> tran) {
        super(tran);
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
