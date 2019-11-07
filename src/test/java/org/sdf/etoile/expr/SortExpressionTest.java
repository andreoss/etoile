/*
 * Copyright(C) 2019
 */
package org.sdf.etoile.expr;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link SortExpression}.
 *
 * @since 0.2.0
 */
final class SortExpressionTest {

    @Test
    void canUseColumnName() {
        final String expr = "expr";
        MatcherAssert.assertThat(
            "returns column",
            new SortExpression(expr).get(),
            Matchers.hasToString(expr)
        );
    }

    @Test
    void canUseCast() {
        final Expression exp = new SortExpression("cast(foo as int)");
        MatcherAssert.assertThat(
            "returns expression",
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
            "returns expression with `desc`",
            exp.get(),
            Matchers.hasToString("foo DESC NULLS LAST")
        );
    }

    @Test
    void canSpecifyAscendingOrder() {
        final Expression exp = new SortExpression(
            "foo:asc"
        );
        MatcherAssert.assertThat(
            "returns expression with `asc`",
            exp.get(),
            Matchers.hasToString("foo ASC NULLS FIRST")
        );
    }

    @Test
    void failsWhenExpressionMalformed() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            new SortExpression("foo:???")::get
        );
    }
}
