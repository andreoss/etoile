/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;

/**
 * Strigified with header added.
 *
 * @param <T> Underlying data type.
 * @since 0.2.0
 */
public final class StringifiedWithHeader<T> extends TransformationEnvelope<Row> {

    /**
     * Ctor.
     * @param source Original transformation.
     */
    StringifiedWithHeader(final Transformation<T> source) {
        super(() ->
            new OrderedUnion<>(
                new Header<>(source),
                new Stringified<>(source)
            )
        );
    }
}
