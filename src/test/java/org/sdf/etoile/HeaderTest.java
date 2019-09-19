package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

final class HeaderTest extends SparkTestTemplate {
    @Test
    void returnsHeaderAsDataset() {
        MatcherAssert.assertThat(
                "returns header of dataset as dataset",
                new Collected<>(new Header<>(
                        new FakeInput(session,
                                "id int, val string, num decimal(38,12)"
                        )
                )),
                Matchers.hasItem(
                        Matchers.hasToString("[id,val,num]")
                )
        );
    }
}
