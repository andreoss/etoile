/*
 * Copyright(C) 2019
 */
package org.sdf.etoile.util;

import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link MappedKeysMap}.
 *
 * @since 0.2.5
 */
final class MappedKeysMapTest {
    @Test
    void mapsKeys() {
        final String value = "bar";
        MatcherAssert.assertThat(
            "maps keys",
            new MappedKeysMap<>(
                String::toUpperCase,
                Collections.singletonMap("foo", value)
            ),
            Matchers.hasEntry("FOO", value)
        );
    }
}
