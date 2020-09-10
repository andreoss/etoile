/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.sdf.etoile.expr.ExpressionOf;

/**
 * Add a column with data file location.
 *
 * @param <X> Underlying type of original tranformation.
 * @since 0.6.0
 */
final class InputFileNames<X> extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     * @param col Input file column name.
     * @param original Transformation to add column to.
     */
    InputFileNames(final String col, final Transformation<X> original) {
        super(
            () -> new WithColumns(
                original,
                new ExpressionOf(
                    functions.input_file_name().as(col)
                )
            )
        );
    }

}

