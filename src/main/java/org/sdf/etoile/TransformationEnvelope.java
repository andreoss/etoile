/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;

/**
 * An Envelope for @{link Transformation}.
 *
 * @param <Y> Underlying data type
 * @since 0.1.0
 */
@RequiredArgsConstructor
public abstract class TransformationEnvelope<Y> implements Transformation<Y> {
    /**
     * A factory for Transformation.
     */
    private final Transformation<Y> factory;

    /**
     * Secondary ctor.
     *
     * @param set A dataset
     */
    TransformationEnvelope(final Dataset<Y> set) {
        this(() -> set);
    }

    @Override
    public final Dataset<Y> get() {
        return this.factory.get();
    }
}
