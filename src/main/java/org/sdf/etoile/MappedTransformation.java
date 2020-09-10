/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A transformation with map functions applied to it.
 *
 * @since 0.1.0
 */
@RequiredArgsConstructor
public final class MappedTransformation implements Transformation<Row> {
    /**
     * An original transformation.
     */
    private final Transformation<Row> orig;

    /**
     * A map function.
     */
    private final MapFunction<Row, Row> map;

    @Override
    public Dataset<Row> get() {
        final Dataset<Row> dataset = this.orig.get();
        return dataset.map(this.map, dataset.exprEnc());
    }
}
