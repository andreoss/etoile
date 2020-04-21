/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link StringifiedWithHeader}.
 * @since 0.2.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class StringifiedWithHeaderTest extends SparkTestTemplate {
    /**
     * Adds header for resulting file for empty tranformation.
     * @throws IOException on error
     */
    @Test
    void addsHeaderForEmptyOutput() throws IOException {
        final Path output = this.temp.newFolder("output")
            .toPath()
            .resolve("csv");
        MatcherAssert.assertThat(
            "writes csv with header",
            new CsvText(
                new Saved<>(
                    output.toUri(),
                    new FormatOutput<>(
                        new StringifiedWithHeader<>(
                            new FakeInput(this.session, "id int, name string")
                        ),
                        new MapOf<>()
                    )
                )
            ),
            new LinesAre(
                "id,name"
            )
        );
    }

    /**
     * Adds header for resulting file.
     * @throws IOException on error
     */
    @Test
    void addsHeader() throws IOException {
        final Path output = this.temp.newFolder("output")
            .toPath()
            .resolve("csv");
        MatcherAssert.assertThat(
            "writes csv with header",
            new CsvText(
                new Saved<>(
                    output,
                    new FormatOutput<>(
                        new FakeInput(
                            this.session,
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
                new FakeInput(this.session, "id int"),
                new MapOf<>(
                    new MapEntry<>("header", "true"),
                    new MapEntry<>("format", "csv+header")
                )
            )
        );
    }
}
