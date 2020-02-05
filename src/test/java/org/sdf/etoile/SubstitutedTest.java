/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.sql.Timestamp;
import java.util.Arrays;
import org.apache.spark.sql.Row;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Substituted}.
 * @since 0.3.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class SubstitutedTest extends SparkTestTemplate {
    @Test
    void checksIfMapIsSerializable() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new Substituted(
                () -> {
                    throw new IllegalStateException("ok");
                },
                new MapOf<>()
            ).get()
        );
    }

    @Test
    void replacesStringValue() {
        final Transformation<Row> replaced = new Substituted(
            new FakeInput(
                this.session,
                "id int, name string",
                Arrays.asList(
                    Factory.arrayOf(1, "foo"),
                    Factory.arrayOf(2, "bar"),
                    Factory.arrayOf(3, "baz")
                )
            ),
            new ReplacementMap("string:foo/XXX")
        );
        MatcherAssert.assertThat(
            "replaced foo with XXX",
            replaced.get()
                .collectAsList(),
            Matchers.contains(
                Matchers.hasToString("[1,XXX]"),
                Matchers.hasToString("[2,bar]"),
                Matchers.hasToString("[3,baz]")
            )
        );
    }

    @Test
    void replacesMultipleStringValues() {
        final Transformation<Row> replaced = new Substituted(
            new FakeInput(
                this.session,
                "id int, name string",
                Arrays.asList(
                    Factory.arrayOf(1, "foo"),
                    Factory.arrayOf(2, "bar"),
                    Factory.arrayOf(3, "baz")
                )
            ),
            new ReplacementMap("string:foo/GOA,string:baz/TSE")
        );
        MatcherAssert.assertThat(
            "replaced foo with XXX",
            replaced.get()
                .collectAsList(),
            Matchers.contains(
                Matchers.hasToString("[1,GOA]"),
                Matchers.hasToString("[2,bar]"),
                Matchers.hasToString("[3,TSE]")
            )
        );
    }

    @Test
    void replacesTsValue() {
        final Transformation<Row> replaced = new Substituted(
            new FakeInput(
                this.session,
                "id int, name timestamp",
                Arrays.asList(
                    Factory.arrayOf(1, Timestamp.valueOf("2000-01-01 00:00:00")),
                    Factory.arrayOf(2, Timestamp.valueOf("1999-01-01 00:00:00")),
                    Factory.arrayOf(3, Timestamp.valueOf("1998-01-01 00:00:00"))
                )
            ),
            new ReplacementMap("timestamp:1999-01-01 00:00:00/null")
        );
        MatcherAssert.assertThat(
            "replaced foo with XXX",
            replaced.get()
                .collectAsList(),
            Matchers.contains(
                Matchers.hasToString("[1,2000-01-01 00:00:00.0]"),
                Matchers.hasToString("[2,null]"),
                Matchers.hasToString("[3,1998-01-01 00:00:00.0]")
            )
        );
    }
}
