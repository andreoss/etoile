package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

public final class PairsTest {
    @Test
    public void parsesStringToPairs() {
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
