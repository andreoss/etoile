package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public final class ExpressionTransformed implements Transformation<Row> {
    private final Transformation<Row> original;
    private final Collection<Map<String, String>> expressions;

    ExpressionTransformed(
            final Transformation<Row> sorted,
            final Map<String, String> opts
    ) {
        this(sorted, opts.getOrDefault("expr", ""));
    }

    private ExpressionTransformed(
            final Transformation<Row> orig,
            final String expr
    ) {
        this(orig, new Pairs(expr));
    }

    @Override
    public Dataset<Row> get() {
        Dataset<Row> dataset = this.original.get();
        for (final Map<String, String> expr : expressions) {
            for (final Map.Entry<String, String> entry : expr.entrySet()) {
                final String column = entry.getKey();
                final String expression = entry.getValue();
                dataset = dataset.withColumn(
                        column, functions.expr(expression)
                );
            }
        }
        return dataset;
    }
}
