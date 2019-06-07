package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public final class PrefixArgsTest {
    @Test
    public void shouldFilterByPerfix() {
        MatcherAssert.assertThat(
                "parses arguments",
                new PrefixArgs("source", new Args(new String[]{"--source.path=/tmp"})),
                Matchers.hasEntry(
                        Matchers.is("path"),
                        Matchers.is("/tmp")
                )
        );
    }
}
