/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import scala.collection.JavaConversions;

/**
 * Alias for column.
 *
 * @since 0.4.0
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@ToString
@Generated
public final class ColumnAlias implements Alias {
    /**
     * Original column name.
     */
    private final String original;

    /**
     * Original alias name.
     */
    private final String renamed;

    /**
     * Ctor.
     * @param expr SQL expresion.
     */
    public ColumnAlias(final String expr) {
        this(
            CatalystSqlParser$.MODULE$.parseExpression(expr)
        );
    }

    /**
     * Ctor.
     * @param expr Spark expression.
     */
    public ColumnAlias(final Expression expr) {
        this(
            JavaConversions.seqAsJavaList(expr.references().toSeq())
                .stream()
                .map(Attribute::name)
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException(
                        String.format("malformed alias expression %s", expr)
                    )
                ),
            ColumnAlias.asNamedExpression(expr).name()
        );
    }

    @Override
    public String reference() {
        return this.original;
    }

    @Override
    public String alias() {
        return this.renamed;
    }

    /**
     * Safe cast to {@link NamedExpression}.
     * @param expr Expression.
     * @return Named expression of cast is possible.
     */
    private static NamedExpression asNamedExpression(final Expression expr) {
        if (expr instanceof NamedExpression) {
            return (NamedExpression) expr;
        }
        throw new IllegalArgumentException(
            String.format("%s is not named", expr)
        );
    }
}
