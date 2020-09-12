/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.function.BooleanSupplier;
import org.cactoos.scalar.Ternary;
import org.cactoos.scalar.Unchecked;

/**
 * Branch between two Transformations on a condition.
 * @param <T> Underlying type
 * @see Transformation
 * @since 0.1.0
 */
final class ConditionalTransformation<T> extends TransformationEnvelope<T> {
    /**
     * Ctor.
     * @param cond A condition
     * @param right When condition holds
     * @param left Otherwise
     */
    ConditionalTransformation(
        final BooleanSupplier cond,
        final Transformation<T> right,
        final Transformation<T> left
    ) {
        super(
            new Unchecked<>(
                new Ternary<>(
                    cond::getAsBoolean,
                    right,
                    left
                )
            ).value()
        );
    }

}
