/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.spark.sql.Row;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Saved}.
 *
 * @since 0.2.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class SavedTest extends SparkTestTemplate {
    /**
     * Writes data to provied location.
     * @throws IOException on error
     */
    @Test
    void savesInCorrectPath() throws IOException {
        final Saved<Row> saved = new Saved<>(
            this.temp.newFolder(),
            new Mode<>(
                "overwrite",
                new FormatOutput<>(
                    new StringifiedWithHeader<>(
                        new FakeInput(
                            this.session(),
                            "id int, val string",
                            Arrays.asList(
                                Factory.arrayOf(1, "abc"),
                                Factory.arrayOf(2, "abc")
                            )
                        )
                    ),
                    new MapOf<>()
                )
            )
        );
        MatcherAssert.assertThat(
            "returns path",
            Paths.get(saved.result()).toFile().list(),
            Matchers.not(Matchers.emptyArray())
        );
    }

    /**
     * Throws exception in case of unreachable URI.
     *
     * @throws IOException on error
     */
    @Test
    void failsForUnreachableUri() throws IOException {
        final URI path = this.temp.newFolder().toURI();
        final Saved<Row> saved = new Saved<>(
            URI.create(String.format("hdfs://%s", path.getPath())),
            new Mode<>(
                "overwrite",
                new FormatOutput<>(
                    new FakeInput(
                        this.session(),
                        "id int, val string",
                        Arrays.asList(
                            Factory.arrayOf(1, "abc"),
                            Factory.arrayOf(2, "abc")
                        )
                    ),
                    new MapOf<>()
                )
            )
        );
        MatcherAssert.assertThat(
            "fails on hdfs uri",
            Assertions.assertThrows(Exception.class, saved::result),
            Matchers.hasProperty("message", Matchers.startsWith("Incomplete HDFS URI"))
        );
    }
}
