/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AllOf;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Okay}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class OkayTest {

    @Test
    void shouldAlwaysBeTrueAndHasDescription() {
        MatcherAssert.assertThat(
            new Okay("hey"),
            new IsOkay(
                Matchers.equalToIgnoringCase("HEY")
            )
        );
    }

    @Test
    void shouldAlwaysBeTrue() {
        MatcherAssert.assertThat(
            new Okay(),
            new AllOf<>(
                new IsNot<>(new IsMismatch()),
                new IsOkay()
            )
        );
    }

    @Test
    void shouldHaveToString() {
        MatcherAssert.assertThat(
            new Okay("whatever"),
            Matchers.hasToString(
                Matchers.containsString("whatever")
            )
        );
    }

    @Test
    void shouldBeEqualToItselfByHashCodeWithArg() {
        MatcherAssert.assertThat(
            new Okay("hello").hashCode(),
            Matchers.is(new Okay("hello").hashCode())
        );
    }

    @Test
    void shouldBeEqualToItselfByHashCode() {
        MatcherAssert.assertThat(
            new Okay().hashCode(),
            Matchers.is(new Okay().hashCode())
        );
    }

}
