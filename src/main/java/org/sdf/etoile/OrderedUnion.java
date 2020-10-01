/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.sdf.etoile.expr.ExpressionOf;

/**
 * Ordered union of two {@link Transformation}s.
 * Left side goes first.
 * @param <Y> Underlying data type.
 * @since 0.3.1
 */
final class OrderedUnion<Y> extends TransformationEnvelope<Row> {

    /**
     * Ctor.
     * @param left Left side.
     * @param right Right side.
     */
    OrderedUnion(final Transformation<Y> left, final Transformation<Y> right) {
        this(left, right, "__order");
    }

    /**
     * Ctor.
     * @param left Left side.
     * @param right Right side.
     * @param pseudo Column name for order clause.
     */
    private OrderedUnion(
        final Transformation<Y> left,
        final Transformation<Y> right,
        final String pseudo
    ) {
        super(
            new NumberedPartitions<>(
                new WithoutColumns<>(
                    new Sorted<>(
                        new Union<>(
                            new WithColumns(
                                left,
                                new ExpressionOf(
                                    functions
                                        .lit(0)
                                        .as(pseudo)
                                )
                            ),
                            new WithColumns(
                                right,
                                new ExpressionOf(
                                    functions
                                        .monotonically_increasing_id()
                                        .as(pseudo)
                                )
                            )
                        ),
                        pseudo
                    ),
                    pseudo
                ),
                1
            )
        );
    }
}
