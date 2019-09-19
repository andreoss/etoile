package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public final class MissingUDFTest {
    @Test
    public void trimsValuesLeft() {
        MatcherAssert.assertThat(
                "[spaces]\\u0001 = MISSING",
                new MissingUDF().call("    \u0001"),
                Matchers.equalTo("MISSING")
        );
    }

    @Test
    public void doesNothingWithNonEmptyString() {
        MatcherAssert.assertThat(
                "X = X",
                new MissingUDF().call("X"),
                Matchers.equalTo("X")
        );
    }

    @Test
    public void trimsValuesRight() {
        MatcherAssert.assertThat(
                "\\u0001[spaces] = MISSING",
                new MissingUDF().call("\u0001   "),
                Matchers.equalTo("MISSING")
        );
    }

    @Test
    public void replacesMissingValueWithConstant() {
        MatcherAssert.assertThat(
                "\\u0001 = MISSING",
                new MissingUDF().call("\u0001"),
                Matchers.equalTo("MISSING")
        );
    }
}