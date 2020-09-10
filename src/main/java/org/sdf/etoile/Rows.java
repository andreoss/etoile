/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cactoos.list.ListOf;

/**
 * Transformation from pre-constructed {@link Row}s.
 *
 * @since 0.1.0
 */
@RequiredArgsConstructor
public final class Rows implements Transformation<Row> {
    /**
     * Spark session.
     */
    private final SparkSession spark;

    /**
     * Schema of transformation.
     */
    private final Schema ddl;

    /**
     * Rows.
     */
    private final List<Row> elements;

    /**
     * Secondary ctor.
     *
     * @param session A Spark session
     * @param schema A schema of resulting dataset
     * @param rows An array of rows
     */
    Rows(final SparkSession session, final Schema schema, final Row... rows) {
        this(session, schema, new ListOf<>(rows));
    }

    @Override
    public Dataset<Row> get() {
        return this.spark.createDataFrame(this.elements, this.ddl.get());
    }
}
