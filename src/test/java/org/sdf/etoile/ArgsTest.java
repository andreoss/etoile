/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Args}.
 *
 * @since 0.1.0
 */
final class ArgsTest {
    /**
     * Parses single argument to map.
     */
    @Test
    void parsesInputArg() {
        MatcherAssert.assertThat(
            "parses arguments",
            new Args("--foo=bar"),
            Matchers.hasEntry(
                Matchers.is("foo"),
                Matchers.is("bar")
            )
        );
    }

    /**
     * Should handle spaces in value.
     */
    @Test
    void parsesArgsWithSpaces() {
        MatcherAssert.assertThat(
            "parses argument with spaces and parenthesis",
            new Args("--input.sort=cast(AAA as int)"),
            Matchers.hasEntry(
                Matchers.is("input.sort"),
                Matchers.is("cast(AAA as int)")
            )
        );
    }

    /**
     * Gracefully handles unparsable string.
     */
    @Test
    void failsOnUnparsableArg() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Args(new String[]{"--yolo"}).get("yolo")
        );
    }
}
