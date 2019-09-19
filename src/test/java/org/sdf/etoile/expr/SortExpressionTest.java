package org.sdf.etoile.expr;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

final class SortExpressionTest {

    @Test
    void canUseColumnName() {
        final Expression exp = new SortExpression("foo");
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("foo")
        );
    }

    @Test
    void canUseCast() {
        final Expression exp = new SortExpression("cast(foo as int)");
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("CAST(foo AS INT)")
        );
    }

    @Test
    void canSpecifyOrder() {
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
    void canSpecifyOrder_Asc() {
        final Expression exp = new SortExpression(
                "foo:asc"
        );
        MatcherAssert.assertThat(
                "returns column",
                exp.get(),
                Matchers.hasToString("foo ASC NULLS FIRST")
        );
    }

    @Test
    void canSpecifyOrder_failOnIncorrect() {
        Assertions.assertThrows(IllegalArgumentException.class, new SortExpression("foo:???")::get);
    }
}
