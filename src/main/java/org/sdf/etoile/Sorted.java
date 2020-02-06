/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.sdf.etoile.expr.Expression;
import org.sdf.etoile.expr.SortExpression;

/**
 * Sorted {@link Transformation}.
 * @param <Y> Underlying data type.
 * @since 0.2.0
 */
@RequiredArgsConstructor
final class Sorted<Y> implements Transformation<Y> {
    /**
     * Original transformation.
     */
    private final Transformation<Y> original;

    /**
     * Expressions to sort by.
     */
    private final List<Expression> expressions;

    /**
     * Secondary ctor.
     * @param orig Original transformation.
     * @param expr Exrpessions.
     */
    Sorted(final Transformation<Y> orig, final String... expr) {
        this(orig, Arrays.stream(expr)
            .map(SortExpression::new)
            .collect(Collectors.toList()));
    }

    @Override
    public Dataset<Y> get() {
        final Column[] columns = this.expressions.stream()
            .map(Expression::get)
            .toArray(Column[]::new);
        return this.original.get()
            .sort(columns);
    }
}
