package org.sdf.etoile.expr;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

@RequiredArgsConstructor
public final class SortExpression implements Expression {
    private final String expr;

    @Override
    public Column get() {
        final String[] parts = this.expr.split(":");
        final Column result;
        if (parts.length == 2) {
            result = new OrderExpression(
                    new SortExpression(parts[0]),
                    parts[1].toLowerCase()
            ).get();
        } else {
            result = functions.expr(this.expr);
        }
        return result;
    }
}
