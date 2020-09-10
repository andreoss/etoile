/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link PrefixArgs}.
 *
 * @since 0.1.0
 */
final class PrefixArgsTest {
    /**
     * Should remove prefixes the from original map.
     */
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
