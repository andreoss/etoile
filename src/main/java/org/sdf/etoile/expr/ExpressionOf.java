/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.expr;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Expression.
 *
 * @since 0.6.0
 */
@RequiredArgsConstructor
public final class ExpressionOf implements Expression {
    /**
     * Expression.
     */
    private final Column expr;

    /**
     * Secondary ctor.
     * @param expr Expression.
     */
    public ExpressionOf(final String expr) {
        this(functions.expr(expr));
    }

    @Override
    public Column get() {
        return this.expr;
    }
}
