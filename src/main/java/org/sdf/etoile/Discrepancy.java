/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;

/**
 * Discrepancy between two datasets.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
public final class Discrepancy implements Transformation<Row> {
    /**
     * Join keys.
     */
    private final List<String> keys;

    /**
     * Right dataset.
     */
    private final Transformation<Row> right;

    /**
     * Left dataset.
     */
    private final Transformation<Row> left;

    /**
     * Filter function.
     *
     * Non-empty result considered a mismatch;
     */
    private final FlatMapFunction<Tuple2<Row, Row>, Row> filter;

    /**
     * Column for result.
     */
    private final String column;

    /**
     * Ctor.
     * @param keys Primary keys.
     * @param right Right dataset.
     * @param left Left dataset.
     * @param filter Compare function.
     * @checkstyle ParameterNumberCheck (10 lines)
     */
    public Discrepancy(
        final List<String> keys,
        final Transformation<Row> right,
        final Transformation<Row> left,
        final FlatMapFunction<Tuple2<Row, Row>, Row> filter
    ) {
        this(keys, right, left, filter, "__result");
    }

    @Override
    public Dataset<Row> get() {
        final Dataset<Row> rds = this.right.get().withColumn(this.column, functions.lit("?"));
        final Dataset<Row> lds = this.left.get().withColumn(this.column, functions.lit("?"));
        return rds.joinWith(
            lds,
            this.buildJoinCondition(rds, lds),
            "outer"
        ).flatMap(this.filter, rds.exprEnc());
    }

    /**
     * Builds a join expression.
     * @param rds Right.
     * @param lds Left.
     * @return Join conditional expression
     */
    private Column buildJoinCondition(final Dataset<Row> rds, final Dataset<Row> lds) {
        return this.keys
            .stream()
            .map(k -> rds.col(k).eqNullSafe(lds.col(k)))
            .reduce(functions.expr("1==1"), Column::and);
    }
}
