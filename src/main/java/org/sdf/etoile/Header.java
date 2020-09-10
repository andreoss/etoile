/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * Header (column names) of transformation.
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
        super(
            () -> new Rows(
                dataset.sparkSession(),
                new SchemaOf<>(new Stringified<>(dataset)),
                new GenericRow(dataset.schema().fieldNames())
            )
        );
    }
}
