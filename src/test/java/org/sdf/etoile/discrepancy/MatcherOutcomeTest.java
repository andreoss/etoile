/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link MatcherOutcome}.
 *
 * @since 0.7.0
 */
final class MatcherOutcomeTest {
    @Test
    void shouldBeMismatchIfFailsToMatch() {
        final Outcome outcome = new MatcherOutcome(
            Matchers.is(123),
            321
        );
        Assertions.assertAll(
            () -> MatcherAssert.assertThat(
                outcome.isOkay(),
                Matchers.is(false)
            ),
            () -> MatcherAssert.assertThat(
                outcome.description(),
                Matchers.is("is <123> => was <321>")
            )
        );
    }

    @Test
    void shouldBeOkayIfMatches() {
        final Outcome outcome = new MatcherOutcome(
            Matchers.notNullValue(),
            new Object()
        );
        Assertions.assertAll(
            () -> MatcherAssert.assertThat(
                outcome.isOkay(),
                Matchers.is(true)
            ),
            () -> MatcherAssert.assertThat(
                outcome.description(),
                Matchers.is("OK")
            )
        );
    }
}
