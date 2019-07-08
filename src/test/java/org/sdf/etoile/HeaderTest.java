package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public final class HeaderTest extends SparkTestTemplate {
    @Test
    public void returnsHeaderAsDataset() {
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
