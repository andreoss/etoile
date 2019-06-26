package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.sdf.etoile.expr.Expression;
import org.sdf.etoile.expr.SortExpression;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
final class Sorted<Y> implements Transformation<Y> {
    private final Transformation<Y> df;
    private final List<Expression> expressions;

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
        return this.df.get()
                .sort(columns);
    }
}
