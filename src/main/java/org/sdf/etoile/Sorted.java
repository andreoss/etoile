package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.sdf.etoile.expr.Expression;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
final class Sorted<Y> implements Transformation<Y> {
    private final Transformation<Y> df;
    private final List<Expression> expressions;

    Sorted(final Transformation<Y> df, final String... expr) {
        this(df, Arrays.stream(expr)
                .map(SortExpression::new)
                .collect(Collectors.toList()));
    }

    private Sorted(final Transformation<Y> df, final Expression expr) {
        this(df, Collections.singletonList(expr));
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
