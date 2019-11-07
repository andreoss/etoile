/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

/**
 * Fake Input.
 *
 * @since 0.2.0
 */
@RequiredArgsConstructor
final class FakeInput implements Transformation<Row> {
    /**
     * Spark session.
     */
    private final SparkSession session;

    /**
     * Schema.
     */
    private final StructType schema;

    /**
     * Rows.
     */
    private final List<Row> rows;

    /**
     * Secondary ctor.
     *
     * @param session Spark session
     * @param ddl Ddl expression for schema
     */
    FakeInput(final SparkSession session, final String ddl) {
        this(session, StructType.fromDDL(ddl));
    }

    /**
     * Secondary ctor.
     * Creates empty Tranformation.
     *
     * @param session Spark session
     * @param schm Schema of dataset
     */
    private FakeInput(final SparkSession session, final StructType schm) {
        this(session, schm, Collections.emptyList());
    }

    /**
     * Secondary ctor.
     *
     * @param session Spark session
     * @param ddl Ddl extpression
     * @param rows Rows
     */
    FakeInput(final SparkSession session, final String ddl, final List<Object[]> rows) {
        this(
            session,
            StructType.fromDDL(ddl),
            rows.stream()
                .map(row -> new GenericRowWithSchema(row, StructType.fromDDL(ddl)))
                .collect(Collectors.toList())
        );
    }

    @Override
    public Dataset<Row> get() {
        return this.session.createDataFrame(this.rows, this.schema);
    }

}
