package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

final class ArgsTest {
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

    @Test
    void failsOnUnparsableArg() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[]{"--foo"}).get("foo")
        );
    }
}
