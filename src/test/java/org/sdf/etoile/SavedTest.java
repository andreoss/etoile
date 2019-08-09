package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;

public final class SavedTest extends SparkTestTemplate {
    @Test
    public void savesInCorrectPath() throws IOException {
        final URI path = temp.newFolder().toURI();
        final Saved<Row> saved = new Saved<>(
                path,
                new Mode<>(
                        "overwrite",
                        new HeaderCsvOutput<>(
                                new FakeInput(
                                        session,
                                        "id int, val string",
                                        Arrays.asList(
                                                Factory.arrayOf(1, "abc"),
                                                Factory.arrayOf(2, "abc")
                                        )
                                )
                        )
                )
        );
        MatcherAssert.assertThat(
                "returns path",
                Paths.get(saved.result()).toFile().list(),
                Matchers.not(Matchers.emptyArray())
        );
    }

    @Test(expected = IOException.class)
    public void failsForUnreachableUri() throws IOException {
        final URI path = temp.newFolder().toURI();
        final Saved<Row> saved = new Saved<>(
                URI.create("hdfs://" + path.getPath()),
                new Mode<>(
                        "overwrite",
                        new HeaderCsvOutput<>(
                                new FakeInput(
                                        session,
                                        "id int, val string",
                                        Arrays.asList(
                                                Factory.arrayOf(1, "abc"),
                                                Factory.arrayOf(2, "abc")
                                        )
                                )
                        )
                )
        );
        try {
            saved.result();
        } catch (final Exception ex) {
            MatcherAssert.assertThat(
                    "fails on hdfs uri",
                    ex.getMessage(),
                    Matchers.startsWith("Incomplete HDFS URI")
            );
            throw ex;
        }
    }
}