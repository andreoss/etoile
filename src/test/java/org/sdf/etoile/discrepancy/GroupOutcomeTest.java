/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link GroupOutcome}.
 *
 * @since 0.7.0
 */
final class GroupOutcomeTest {
    @Test
    void shouldBeNotOkayIfOneIsNotOkay() {
        MatcherAssert.assertThat(
            new GroupOutcome(
                new Okay(),
                new Okay(),
                new Mismatch("???"),
                new Okay()
            ),
            new IsMismatch(
                Matchers.startsWith("Failure(1/4")
            )
        );
    }

    @Test
    void shouldBeOkayIfAllOkay() {
        MatcherAssert.assertThat(
            new GroupOutcome(
                new Okay(),
                new Okay(),
                new Okay(),
                new Okay()
            ),
            new IsOkay("OK(4)")
        );
    }
}
