/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Header}.
 * @since 0.2.1
 */
final class HeaderTest extends SparkTestTemplate {
    @Test
    void returnsHeaderAsDataset() {
        MatcherAssert.assertThat(
            "returns header of dataset as dataset",
            new Collected<>(
                new Header<>(
                    new FakeInput(
                        this.session,
                        "id int, val string, num decimal(38,12)"
                    )
                )
            ),
            Matchers.hasItem(
                Matchers.hasToString("[id,val,num]")
            )
        );
    }
}
