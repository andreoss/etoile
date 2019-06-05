package org.sdf.etoile;

import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

public final class MainTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(expected = NullPointerException.class)
    public void requiresArguments() {
        new Main(
                Mockito.mock(SparkSession.class),
                Collections.emptyMap()
        ).run();
    }

    @Test
    public void startsSpark() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.newFolder("output").toPath().resolve("csv").toFile();
        Files.copy(
                Paths.get(
                        this.getClass()
                                .getClassLoader()
                                .getResource("test.avro")
                                .getFile()
                ),
                input.toPath().resolve("test.avro")
        );
        new Main(
                SparkSession.builder()
                        .master("local[1]")
                        .getOrCreate(),
                new Args(
                        "--input=" + input,
                        "--output=" + output
                )
        ).run();
        MatcherAssert.assertThat(
                "output created",
                output,
                Matchers.hasProperty(
                        "directory", Matchers.is(true)
                )
        );
    }
}
