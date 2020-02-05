/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Test;

/**
 * Test for  {@link Pairs}.
 *
 * @since 0.1.0
 */
final class PairsTest {
    @Test
    void parsesStringToPairs() {
        MatcherAssert.assertThat(
            "splits string into pairs",
            new Pairs(",", ":", "a:b,c:d,foo:bar"),
            Matchers.contains(
                IsMapContaining.hasEntry("a", "b"),
                IsMapContaining.hasEntry("c", "d"),
                IsMapContaining.hasEntry("foo", "bar")
            )
        );
    }
}
