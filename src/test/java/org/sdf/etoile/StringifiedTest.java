/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.math.BigDecimal;
import java.util.Arrays;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Stringified}.
 *
 * @since 0.3.2
 * @checkstyle MagicNumberCheck (30 lines)
 */
final class StringifiedTest extends SparkTestTemplate {
    @Test
    void stringifiesSchema() {
        final String type = "string";
        MatcherAssert.assertThat(
            "all columns are strings",
            new SchemaOf<>(
                new Stringified<>(
                    new FakeInput(
                        this.session,
                        "id int, name string, val decimal(38,12)",
                        Arrays.asList(
                            Factory.arrayOf(1, "foo", BigDecimal.valueOf(0.12)),
                            Factory.arrayOf(2, "bar", BigDecimal.valueOf(1.0)),
                            Factory.arrayOf(3, "baz", BigDecimal.valueOf(4.0))
                        )
                    )
                )
            ).asMap(),
            Matchers.allOf(
                IsMapContaining.hasEntry("id", type),
                IsMapContaining.hasEntry("name", type),
                IsMapContaining.hasEntry("val", type)
            )
        );
    }
}
