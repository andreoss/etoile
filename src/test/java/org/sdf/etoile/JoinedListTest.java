package org.sdf.etoile;


import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.sdf.etoile.util.JoinedList;

import java.util.Arrays;

public final class JoinedListTest {
    @Test
    public void joinsLists() {
        MatcherAssert.assertThat(
                "joins two list",
                new JoinedList<>(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5, 6)
                ),
                Matchers.contains(
                        1, 2, 3, 4, 5, 6
                )
        );
    }
}
