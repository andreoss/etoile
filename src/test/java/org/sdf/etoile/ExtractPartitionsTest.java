/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapWithSize;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ExtractPartitions}.
 *
 * @since 0.6.0
 */
final class ExtractPartitionsTest {
    @Test
    void stopsAfterAllPartitionsCollected() {
        MatcherAssert.assertThat(
            new ExtractPartitions()
                .apply("/foo=garbage/bar/a=1/more/z=2/b=3/1.csv"),
            Matchers.allOf(
                Matchers.hasEntry("z", "2"),
                Matchers.hasEntry("b", "3")
            )
        );
    }

    @Test
    void emptyString() {
        MatcherAssert.assertThat(
            new ExtractPartitions()
                .apply(""),
            IsMapWithSize.anEmptyMap()
        );
    }

    @Test
    void noPartitions() {
        MatcherAssert.assertThat(
            new ExtractPartitions()
                .apply("/foo/bar/1.csv"),
            IsMapWithSize.anEmptyMap()
        );
    }

    @Test
    void extractsPartitionsAMap() {
        MatcherAssert.assertThat(
            new ExtractPartitions()
                .apply("/foo/bar/a=1/b=3/1.csv"),
            Matchers.allOf(
                Matchers.hasEntry("a", "1"),
                Matchers.hasEntry("b", "3")
            )
        );
    }
}

