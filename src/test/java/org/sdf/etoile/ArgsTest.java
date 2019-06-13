package org.sdf.etoile;

import org.hamcrest.Matchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class ArgsTest {
    @Test
    public void parsesInputArg() {
        MatcherAssert.assertThat(
                "parses arguments",
                new Args(new String[]{"--foo=bar"}),
                Matchers.hasEntry(
                        Matchers.is("foo"),
                        Matchers.is("bar")
                )
        );
    }

    @Test
    public void parsesArgsWithSpaces() {
        MatcherAssert.assertThat(
                "parses argument with spaces and parentesis",
                new Args(new String[]{"--input.sort=cast(AAA as int)"}),
                Matchers.hasEntry(
                        Matchers.is("input.sort"),
                        Matchers.is("cast(AAA as int)")
                )
        );
    }
}
