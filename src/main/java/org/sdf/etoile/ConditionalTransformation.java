package org.sdf.etoile;

import java.util.function.BooleanSupplier;

final class ConditionalTransformation<T> extends Transformation.Envelope<T> {
    ConditionalTransformation(
            final BooleanSupplier cond,
            final Transformation<T> right,
            final Transformation<T> left
    ) {
        super(() -> {
            if (cond.getAsBoolean()) {
                return right;
            } else {
                return left;
            }
        });
    }

}
