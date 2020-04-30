/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Detailed}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class DetailedTest {
    @Test
    void shouldHaveToString() {
        MatcherAssert.assertThat(
            new Detailed("???", new Mismatch("wtf")),
            Matchers.hasToString(
                Matchers.containsString(new Mismatch("???: wtf").toString())
            )
        );
    }

    @Test
    void shouldAddDescriptionToMismatch() {
        MatcherAssert.assertThat(
            new Detailed("xxx", new Mismatch("wtf")),
            new IsMismatch("xxx: wtf")
        );
    }

    @Test
    void shouldBeEqualToOkayBByHashCode() {
        MatcherAssert.assertThat(
            new Detailed("yyy", new Okay("all good")).hashCode(),
            Matchers.is(new Okay("yyy: all good").hashCode())
        );
    }

    @Test
    void shouldBeEqualToOkay() {
        MatcherAssert.assertThat(
            new Detailed("yyy", new Okay("all good")),
            Matchers.allOf(
                Matchers.equalToObject(new Okay("yyy: all good")),
                Matchers.not(
                    new Mismatch("yyy: all good")
                )
            )
        );
    }

    @Test
    void shouldAddDescriptionToOkay() {
        MatcherAssert.assertThat(
            new Detailed("yyy", new Okay("all good")),
            new IsOkay("yyy: all good")
        );
    }

}
