/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ColumnAlias}.
 *
 * @since 0.4.0
 */
final class ColumnAliasTest {
    @Test
    void hasToString() {
        final Alias alias = new ColumnAlias("id as x");
        MatcherAssert.assertThat(
            "should have correct toString method",
            alias,
            Matchers.hasToString(
                "ColumnAlias(original=id, renamed=x)"
            )
        );
    }

    @Test
    void shouldHaveProperHashCode() {
        final String expr = "id as bar";
        final Alias alias = new ColumnAlias(expr);
        MatcherAssert.assertThat(
            "should have proper hashCode",
            alias.hashCode(),
            Matchers.allOf(
                Matchers.is(new ColumnAlias(expr).hashCode()),
                Matchers.not(new ColumnAlias("id as baz").hashCode())
            )
        );
    }

    @Test
    void equalsToItself() {
        final String expr = "name as nomen";
        final Alias alias = new ColumnAlias(expr);
        MatcherAssert.assertThat(
            "equals to itsefl",
            alias,
            Matchers.allOf(
                Matchers.is(new ColumnAlias(expr)),
                Matchers.not(new ColumnAlias("nombre as nomen")),
                Matchers.not(Matchers.nullValue()),
                Matchers.not(new Object()),
                Matchers.is(alias)
            )
        );
    }

    @Test
    void handlesIllegalExpression() {
        Assertions.assertThrows(
            IllegalArgumentException.class, () ->
                new ColumnAlias("0 as bar")
        );
    }

    @Test
    void parsesAliasExpression() {
        final Alias alias = new ColumnAlias("foo as bar");
        MatcherAssert.assertThat(
            "should create alias",
            Arrays.asList(alias.reference(), alias.alias()),
            Matchers.contains("foo", "bar")
        );
    }
}

