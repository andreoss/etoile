package org.sdf.etoile;

import java.util.function.BooleanSupplier;

public final class ConditionalTransformation<T> extends Transformation.Envelope<T> {
    public ConditionalTransformation(
            final BooleanSupplier cond,
            final Transformation<T> right,
            final Transformation<T> left
    ) {
        super(() -> cond.getAsBoolean() ? right : left);
    }

}
