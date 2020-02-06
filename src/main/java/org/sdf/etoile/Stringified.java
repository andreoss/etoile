/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType$;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

/**
 * A tranformation which each column converted to `string`.
 * {@see org.apache.spark.sql.types.StringType}
 *
 * @param <Y> Underlying data type.
 * @since 0.3.2
 */
public final class Stringified<Y> extends TransformationEnvelope<Row> {

    /**
     * Ctor.
     * @param original Dataset.
     */
    public Stringified(final Dataset<Y> original) {
        this(new Noop<>(original));
    }

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
