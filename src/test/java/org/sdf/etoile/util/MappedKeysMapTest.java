package org.sdf.etoile.util;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Collections;

final class MappedKeysMapTest {
    @Test
    void mapsKeys() {
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
