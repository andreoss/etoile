/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Collection;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Tranformation with expressions applied.
 *
 * @since 0.3.5
 */
@RequiredArgsConstructor
public final class ExpressionTransformed implements Transformation<Row> {
    /**
     * Original transformation.
     */
    private final Transformation<Row> original;

    /**
     * Expressions.
     */
    private final Collection<Map<String, String>> expressions;

    /**
     * Ctor.
     * @param orig Original.
     * @param opts Parameters
     */
    ExpressionTransformed(final Transformation<Row> orig,
        final Map<String, String> opts) {
        this(orig, opts.getOrDefault("expr", ""));
    }

    /**
     * Ctor.
     * @param orig Original.
     * @param expr Expressions as comma-separater string.
     */
    private ExpressionTransformed(
        final Transformation<Row> orig,
        final String expr
    ) {
        this(orig, new Pairs(expr));
    }

    @Override
    public Dataset<Row> get() {
        Dataset<Row> memo = this.original.get();
        for (final Map<String, String> expr : this.expressions) {
            for (final Map.Entry<String, String> entry : expr.entrySet()) {
                final String column = entry.getKey();
                final String expression = entry.getValue();
                memo = memo.withColumn(
                    column, functions.expr(expression)
                );
            }
        }
        return memo;
    }
}
