package org.sdf.etoile;


import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

final class VerifyTest {
    @Test
    void shouldCheckAndFailsWhenNot() {
        final Object obj = new Object();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Verify.isSerializable(obj)
        );
    }

    @Test
    void shouldCheck() {
        final Object obj = new Serializable() {
        };
        MatcherAssert.assertThat(
                "checks for being Serializable",
                Verify.isSerializable(obj),
                Matchers.is(obj)
        );
    }
}