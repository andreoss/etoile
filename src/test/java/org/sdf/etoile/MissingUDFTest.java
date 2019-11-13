/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link MissingUDF}.
 *
 * @since 0.3.2
 * @checkstyle AvoidDuplicateLiterals (100 lines)
 * @checkstyle AbbreviationAsWordInNameCheck (3 lines)
 */
final class MissingUDFTest {
    /**
     * Default literal.
     */
    private static final String MISSING = "DEFAULT_VALUE";

    /**
     * Should trim value if needed.
     */
    @Test
    void trimsValuesLeft() {
        MatcherAssert.assertThat(
            "[spaces]\\u0001 = MISSING",
            new MissingUDF().call("    \u0001"),
            Matchers.equalTo(MissingUDFTest.MISSING)
        );
    }

    /**
     * No-op on mismatch.
     */
    @Test
    void doesNothingWithNonEmptyString() {
        MatcherAssert.assertThat(
            "X = X",
            new MissingUDF().call("X"),
            Matchers.equalTo("X")
        );
    }

    /**
     * Should trim value if needed.
     */
    @Test
    void trimsValuesRight() {
        MatcherAssert.assertThat(
            "\\u0001[spaces] = MISSING",
            new MissingUDF().call("\u0001   "),
            Matchers.equalTo(MissingUDFTest.MISSING)
        );
    }

    /**
     * Replaces with literal.
     */
    @Test
    void replacesMissingValueWithConstant() {
        MatcherAssert.assertThat(
            "\\u0001 = MISSING",
            new MissingUDF().call("\u0001"),
            Matchers.equalTo(MissingUDFTest.MISSING)
        );
    }
}
