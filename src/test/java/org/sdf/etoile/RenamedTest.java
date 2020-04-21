/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Renamed}.
 * @since 0.4.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class RenamedTest extends SparkTestTemplate {
    @Test
    void shouldDoesNothingWhenNoAliases() {
        MatcherAssert.assertThat(
            "should keep names",
            new SchemaOf(
                new Renamed(
                    new FakeInput(SparkTestTemplate.session, "number int, name string")
                )
            ).asMap(),
            Matchers.allOf(
                Matchers.hasEntry("name", "string"),
                Matchers.hasEntry("number", "int")
            )
        );
    }

    @Test
    void renamesColumns() {
        MatcherAssert.assertThat(
            "should rename columns to provided aliases",
            new SchemaOf<>(
                new Renamed(
                    new FakeInput(
                        SparkTestTemplate.session,
                        "number int, name string"
                    ),
                    "name as nombre",
                    "number as numero"
                )
            ).asMap(),
            Matchers.allOf(
                Matchers.hasEntry("nombre", "string"),
                Matchers.hasEntry("numero", "int")
            )
        );
    }
}

