/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;

/**
 * Union of two {@link Transformation}s.
 *
 * @param <Y> Underlying data type.
 * @since 0.3.0
 */
public final class Union<Y> extends Transformation.Envelope<Y> {

    /**
     * Ctor.
     * @param left Left side.
     * @param right Right side.
     */
    public Union(final Transformation<Y> left, final Transformation<Y> right) {
        this(left.get(), right.get());
    }

    /**
     * Ctor.
     * @param left Left side.
     * @param right Right side.
     */
    public Union(final Dataset<Y> left, final Dataset<Y> right) {
        super(() -> left.union(right));
    }
}
