/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link StringifiedWithHeader}.
 * @since 0.2.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class StringifiedWithHeaderTest extends SparkTestTemplate {
    /**
     * Adds header for resulting file for tranformation.
     */
    @Test
    void addsHeader() {
        MatcherAssert.assertThat(
            "writes csv with header",
            new StringifiedWithHeader<>(
                new FakeInput(
                    this.session(),
                    "id int, name string",
                    Factory.arrayOf(1, "hi there")
                )
            ),
            new HasRows<>(
                "[id,name]",
                "[1,hi there]"
            )
        );
    }

}
