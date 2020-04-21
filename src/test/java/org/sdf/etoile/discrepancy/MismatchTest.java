/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */

package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AllOf;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.hamcrest.object.HasEqualValues;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Mismatch}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class MismatchTest {
    @Test
    void shouldAlwaysBeFalseAndHasDescription() {
        MatcherAssert.assertThat(
            new Mismatch("hey"),
            new IsMismatch(
                Matchers.equalToIgnoringCase("HEY")
            )
        );
    }

    @Test
    void shouldAlwaysBeFalse() {
        MatcherAssert.assertThat(
            new Mismatch("wft"),
            new AllOf<>(
                new IsMismatch(),
                new IsNot<>(new IsOkay())
            )
        );
    }

    @Test
    void shouldHaveToString() {
        MatcherAssert.assertThat(
            new Mismatch("whatever"),
            Matchers.hasToString(
                Matchers.containsString("whatever")
            )
        );
    }

    @Test
    void shouldBeEqualToItselfByHashCode() {
        MatcherAssert.assertThat(
            new Mismatch("ffff").hashCode(),
            Matchers.is(new Mismatch("ffff").hashCode())
        );
    }

    @Test
    void shouldBeEqualToItself() {
        MatcherAssert.assertThat(
            new Mismatch("damn"),
            new AllOf<>(
                new HasEqualValues<>(new Mismatch("damn")),
                new IsEqual<>(new Mismatch("damn")),
                new IsNot<>(new IsEqual<>(new Mismatch("why?")))
            )
        );
    }

}
