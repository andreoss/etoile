/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.expr;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;

/**
 * Asc/desc order for an expression.
 *
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class OrderExpression implements Expression {
    /**
     * Expression to order.
     */
    private final Expression expression;

    /**
     * Order modifier.
     */
    private final String order;

    @Override
    public Column get() {
        final Column result;
        final Column column = this.expression.get();
        switch (this.order) {
            case "desc":
                result = column.desc();
                break;
            case "asc":
                result = column.asc();
                break;
            default:
                throw new IllegalArgumentException(this.order);
        }
        return result;
    }
}
