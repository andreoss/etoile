/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

/**
 * A {@link Transformation} with "missing" values removed.
 *
 * Converts each column to string and applies UDF.
 * @see MissingUDF
 * @see Stringified
 * @since 0.3.1
 */
final class Demissingified extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     *
     * @param original Original transformation.
     */
    Demissingified(final Transformation<Row> original) {
        super(
            new ExpressionTransformed(
                new Stringified<>(original),
                new ListOf<>(
                    new MapOf<>(
                        name ->
                            new MapEntry<>(
                                name,
                                String.format("missing(`%s`)", name)
                            ),
                        new SchemaOf<>(original).fieldNames()
                    )
                )
            )
        );
    }
}
