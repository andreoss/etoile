/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.List;
import org.apache.spark.sql.Row;
import org.sdf.etoile.expr.Expression;

/**
 * Add a column with data file location.
 *
 * @since 0.6.0
 */
final class PartitionSchemeValidated extends TransformationEnvelope<Row> {
    /**
     * Secondary ctor.
     * @param original Original transformation.
     * @param expressions Expressions to validate.
     * @param <X> Underlying data type.
     */
    <X> PartitionSchemeValidated(
        final Transformation<Row> original,
        final Expression... expressions
    ) {
        this("__input_file", original, expressions);
    }

    /**
     * Ctor.
     * @param filecolumn Column name.
     * @param original Original transformation.
     * @param expressions Expressions to validate.
     */
    private PartitionSchemeValidated(final String filecolumn,
        final Transformation<Row> original, final Expression... expressions) {
        super(() ->
            new WithColumns(
                new InputFileNames<>(filecolumn, original),
                expressions
            ).get().filter(new Not<>(new PartitionValuesMatch(filecolumn)))
        );
    }

    /**
     * Secondary ctor.
     * @param input Input transformation.
     * @param expressions Expressions.
     */
    PartitionSchemeValidated(final Transformation<Row> input,
        final List<Expression> expressions) {
        this(input, expressions.toArray(new Expression[0]));
    }
}

