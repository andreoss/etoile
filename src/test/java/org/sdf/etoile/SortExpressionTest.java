package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.sdf.etoile.expr.Expression;

public final class SortExpressionTest {

    @Test
    public void canUseColumnName() {
        final Expression exp = new SortExpression( "foo" );
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("foo")
        );
    }

    @Test
    public void canUseCast() {
        final Expression exp = new SortExpression( "cast(foo as int)" );
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("CAST(foo AS INT)")
        );
    }

    @Test
    public void canSpecifyOrder() {
        final Expression exp = new SortExpression(
                "foo:desc"
        );
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("foo DESC NULLS LAST")
        );
    }

    @Test
    public void canSpecifyOrder_Asc() {
        final Expression exp = new SortExpression(
                "foo:asc"
        );
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("foo ASC NULLS FIRST")
        );
    }
}
