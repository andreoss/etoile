/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link HeaderCsvOutput}.
 * @since 0.2.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class HeaderCsvOutputTest extends SparkTestTemplate {
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
                    new HeaderCsvOutput<>(
                        new FakeInput(this.session, "id int, name string")
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
                    new HeaderCsvOutput<>(
                        new FakeInput(
                            this.session,
                            "id int, name string",
                            Arrays.asList(
                                Factory.arrayOf(1, "foo"),
                                Factory.arrayOf(2, "bar")
                            )
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
            new HeaderCsvOutput<>(
                new FakeInput(this.session, "id int"),
                Collections.singletonMap("header", "true")
            )::get
        );
    }
}
