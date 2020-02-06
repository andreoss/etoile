/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import java.net.URI;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Renamed}.
 * @since 0.4.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class RenamedTest extends SparkTestTemplate {
    @Test
    void shouldDoesNothingWhenNoAliases() throws IOException {
        final URI output = this.temp.newFolder().toPath().resolve("csv").toUri();
        MatcherAssert.assertThat(
            "should write unchanged dataset",
            new CsvText(
                new Saved<>(
                    output,
                    new HeaderCsvOutput<>(
                        new Renamed(
                            new FakeInput(this.session, "number int, name string")
                        )
                    )
                )
            ),
            new LinesAre("number,name")
        );
    }

    @Test
    void renamesColumns() throws IOException {
        final URI output = this.temp.newFolder().toPath().resolve("csv").toUri();
        MatcherAssert.assertThat(
            "should rename columns to provided aliases",
            new CsvText(
                new Saved<>(
                    output,
                    new HeaderCsvOutput<>(
                        new Renamed(
                            new FakeInput(
                                this.session,
                                "number int, name string"
                            ),
                            "name as nombre",
                            "number as numbero"
                        )
                    )
                )
            ),
            new LinesAre(
                "numbero,nombre"
            )
        );
    }
}

