package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

final class PrefixArgsTest {
    @Test
    void shouldFilterByPrefix() {
        MatcherAssert.assertThat(
                "parses arguments",
                new PrefixArgs("source", new Args("--source.path=/tmp")),
                Matchers.hasEntry(
                        Matchers.is("path"),
                        Matchers.is("/tmp")
                )
        );
    }
}
