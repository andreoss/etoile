package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

final class MissingUDFTest {
    @Test
    void trimsValuesLeft() {
        MatcherAssert.assertThat(
                "[spaces]\\u0001 = MISSING",
                new MissingUDF().call("    \u0001"),
                Matchers.equalTo("MISSING")
        );
    }

    @Test
    void doesNothingWithNonEmptyString() {
        MatcherAssert.assertThat(
                "X = X",
                new MissingUDF().call("X"),
                Matchers.equalTo("X")
        );
    }

    @Test
    void trimsValuesRight() {
        MatcherAssert.assertThat(
                "\\u0001[spaces] = MISSING",
                new MissingUDF().call("\u0001   "),
                Matchers.equalTo("MISSING")
        );
    }

    @Test
    void replacesMissingValueWithConstant() {
        MatcherAssert.assertThat(
                "\\u0001 = MISSING",
                new MissingUDF().call("\u0001"),
                Matchers.equalTo("MISSING")
        );
    }
}