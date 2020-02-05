/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.sdf.etoile.expr.Expression;

/**
 * With additional columns.
 *
 * @since 0.6.0
 */
final class WithColumns extends TransformationEnvelope<Row> {

    /**
     * Ctor.
     * @param orig Transformation.
     * @param cols Columns to add.
     * @param <X> Underlying data type.
     */
    <X> WithColumns(final Transformation<X> orig, final Expression... cols) {
        super(() -> {
            Dataset<Row> memo = orig.get().toDF();
            for (final Expression col : cols) {
                final NamedExpression named = col.get().named();
                memo = memo.withColumn(
                    named.name(), col.get()
                );
            }
            return memo;
        });
    }
}
