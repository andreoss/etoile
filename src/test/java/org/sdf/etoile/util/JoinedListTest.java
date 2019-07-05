package org.sdf.etoile.util;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

public final class JoinedListTest {
    @Test
    public void joinsLists() {
        MatcherAssert.assertThat(
                "joins two list",
                new Joined<>(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5, 6)
                ),
                Matchers.contains(
                        1, 2, 3, 4, 5, 6
                )
        );
    }
}
