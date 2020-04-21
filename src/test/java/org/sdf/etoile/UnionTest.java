/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Union}.
 *
 * @since 0.4.0
 */
final class UnionTest extends SparkTestTemplate {

    @Test
    void unionOfTwo() throws IOException {
        int id = 0;
        final String schema = "id int, name string";
        MatcherAssert.assertThat(
            "union of two datasets",
            new CsvText(
                new Saved<>(
                    this.temp.newFolder().toPath().resolve("out"),
                    new FormatOutput<>(
                        new Union<>(
                            new FakeInput(
                                this.session,
                                schema,
                                new ListOf<>(
                                    new Object[]{++id, "foo"},
                                    new Object[]{++id, "bar"}
                                )
                            ),
                            new FakeInput(
                                this.session,
                                schema,
                                new ListOf<>(
                                    new Object[]{++id, "baz"},
                                    new Object[]{++id, "fee"}
                                )
                            )
                        ),
                        new MapOf<>(
                            new MapEntry<>("format", "csv+header")
                        )
                    )
                )
            ),
            new LinesAre(
                "id,name",
                "1,foo",
                "2,bar",
                "3,baz",
                "4,fee"
            )
        );
    }
}
