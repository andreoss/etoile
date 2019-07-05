package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

public class StringifiedTest extends SparkTestTemplate {
    @Test
    public void stringifiesSchema() {
        MatcherAssert.assertThat(
                "all columns are string",
                new SchemaOf<>(
                        new Stringified<>(
                                new FakeInput(
                                        session,
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
                        IsMapContaining.hasEntry("id", "string"),
                        IsMapContaining.hasEntry("name", "string"),
                        IsMapContaining.hasEntry("val", "string")
                )
        );
    }
}
