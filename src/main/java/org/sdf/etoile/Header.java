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
 * @param <Y> Underlying type of original transformation.
 * @since 0.2.1
 */
public final class Header<Y> extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     *
     * @param transformation A transformation.
     */
    public Header(final Transformation<Y> transformation) {
        super(
            new Transformation<Row>() {
                private final Dataset<?> dataset = transformation.get();

                @Override
                public Dataset<Row> get() {
                    return new Rows(
                        this.dataset.sparkSession(),
                        new SchemaOf<>(
                            new Stringified<>(this.dataset)
                        ),
                        new GenericRow(
                            this.dataset.schema().fieldNames()
                        )
                    ).get();
                }
            }
        );
    }
}
