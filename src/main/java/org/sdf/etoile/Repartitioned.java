/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sdf.etoile.expr.Expression;
import scala.collection.JavaConversions;

/**
 * Repartitioned.
 * @param <X> Underylying data type.
 * @since 0.6.0
 */
public final class Repartitioned<X> extends TransformationEnvelope<X> {
    /**
     * Ctor.
     * @param original Original transfrormation.
     * @param partitions Partition expressions.
     */
    public Repartitioned(final Transformation<X> original,
        final Expression... partitions) {
        super(() -> {
            return () -> original.get().repartition(
                JavaConversions.asScalaBuffer(
                    Stream.of(partitions)
                        .map(Expression::get)
                        .collect(Collectors.toList())
                )
            );
        });
    }
}
