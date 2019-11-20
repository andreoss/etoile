/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.Serializable;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link SerializableOnly}.
 *
 * @since 0.2.0
 */
final class SerializableOnlyTest {
    /**
     * Should throw for non-serialazible objects.
     */
    @Test
    void shouldCheckAndFailsWhenNot() {
        final Object obj = new Object();
        Assertions.assertThrows(IllegalArgumentException.class, new SerializableOnly<>(obj)::get);
    }

    /**
     * Should return original object if it's serializable.
     */
    @Test
    void shouldCheck() {
        final Object obj = new Serializable() {
        };
        MatcherAssert.assertThat(
            "checks for being Serializable",
            new SerializableOnly<>(obj).get(),
            Matchers.is(obj)
        );
    }
}
