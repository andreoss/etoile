package org.sdf.etoile.util;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;

public final class MappedKeysMapTest {
    @Test
    public void mapsKeys() {
        MatcherAssert.assertThat(
                "maps keys",
                new MappedKeysMap<>(
                        String::toUpperCase,
                        Collections.singletonMap(
                                "foo", "bar"
                        )
                ),
                Matchers.hasEntry("FOO", "bar")
        );
    }
}
