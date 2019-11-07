/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType$;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

/**
 * A tranformation which each column converted to {@link org.apache.spark.sql.types.StringType}.
 *
 * @param <Y> Underlying data type.
 * @since 0.3.2
 */
public final class Stringified<Y> extends Transformation.Envelope<Row> {
    /**
     * Ctor.
     * @param original Original tranfomation.
     */
    public Stringified(final Transformation<Y> original) {
        super(
            new ColumnsCastedToType<Y>(
                original,
                new MapOf<>(
                    f -> new MapEntry<>(
                        f,
                        StringType$.MODULE$.catalogString()
                    ),
                    new SchemaOf<>(original).fieldNames()
                )
            )
        );
    }
}
