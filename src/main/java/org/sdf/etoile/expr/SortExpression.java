/*
 * Copyright(C) 2019
 */
package org.sdf.etoile.expr;

import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * A sort expression for a certain column.
 * Ordered asceding or descending.
 *
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class SortExpression implements Expression {
    /**
     * An column and order expression, can be separated by colon.
     */
    private final String expr;

    @Override
    public Column get() {
        final String[] parts = this.expr.split(":");
        final Column result;
        if (parts.length == 2) {
            result = new OrderExpression(
                new SortExpression(parts[0]),
                parts[1].toLowerCase(Locale.ENGLISH)
            ).get();
        } else {
            result = functions.expr(this.expr);
        }
        return result;
    }
}
