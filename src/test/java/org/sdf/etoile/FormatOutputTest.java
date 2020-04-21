/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import java.util.Arrays;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link FormatOutput}.
 *
 * @since 0.3.2
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class FormatOutputTest extends SparkTestTemplate {
    /**
     * Adds header for resulting file.
     * @throws IOException on error
     */
    @Test
    void addsHeader() throws IOException {
        MatcherAssert.assertThat(
            "writes csv with header",
            new CsvText(
                new Saved<>(
                    this.temp.newFolder("output")
                        .toPath()
                        .resolve("csv"),
                    new FormatOutput<>(
                        new FakeInput(
                            SparkTestTemplate.session,
                            "id int, name string",
                            Arrays.asList(
                                Factory.arrayOf(1, "foo"),
                                Factory.arrayOf(2, "bar")
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
                "2,bar"
            )
        );
    }

    /**
     * Throws {@link IllegalArgumentException} if parameters have `header=true`.
     * @throws IOException on error
     */
    @Test
    void checksParametersForHeaderOptions() throws IOException {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new FormatOutput<>(
                new FakeInput(SparkTestTemplate.session, "id int"),
                new MapOf<>(
                    new MapEntry<>("header", "true"),
                    new MapEntry<>("format", "csv+header")
                )
            )
        );
    }

}
