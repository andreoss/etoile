/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * Extactd header (column names) from transformation.
 *
 * @param <Y> Underlying type of original tranformation.
 * @since 0.2.1
 */
public final class Header<Y> extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     *
     * @param transformation A tranformation.
     */
    public Header(final Transformation<Y> transformation) {
        this(transformation.get());
    }

    /**
     * Secondary ctor.
     *
     * @param dataset A dataset.
     */
    public Header(final Dataset<Y> dataset) {
        super(() -> {
            final Schema schema = new SchemaOf<>(new Stringified<>(dataset));
            return new Rows(
                dataset.sparkSession(),
                schema,
                new GenericRow(schema.get()
                    .fieldNames()
                )
            );
        });
    }
}
