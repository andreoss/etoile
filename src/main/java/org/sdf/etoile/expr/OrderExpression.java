package org.sdf.etoile.expr;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;

@RequiredArgsConstructor
public final class OrderExpression implements Expression {
    private final Expression expression;
    private final String order;

    @Override
    public Column get() {
        final Column result;
        final Column column = this.expression.get();
        switch (order) {
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
