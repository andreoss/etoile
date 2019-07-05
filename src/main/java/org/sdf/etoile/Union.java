package org.sdf.etoile;

import org.apache.spark.sql.Dataset;

public final class Union<Y> extends Transformation.Envelope<Y> {

    public Union(final Transformation<Y> left, final Transformation<Y> right) {
        this(left.get(), right.get());
    }

    public Union(final Dataset<Y> left, final Dataset<Y> right) {
        super(() -> left.union(right));
    }
}
