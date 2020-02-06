/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

/**
 * A {@link Transformation} with "missing" values removed.
 *
 * Convertes each column to string and applies UDF.
 * @see MissingUDF
 * @see Stringified
 * @since 0.3.1
 */
final class Demissingified extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     *
     * @param original Original tranformation.
     */
    Demissingified(final Transformation<Row> original) {
        super(() -> {
            final Schema schema = new SchemaOf<>(original);
            final Map<String, String> map = new MapOf<>(
                name -> new MapEntry<>(
                    name, String.format("missing(`%s`)", name)
                ),
                schema.fieldNames()
            );
            return new ExpressionTransformed(
                new Stringified<>(original), new ListOf<>(map)
            );
        });
    }
}
