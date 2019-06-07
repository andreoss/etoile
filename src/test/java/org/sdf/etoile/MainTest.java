package org.sdf.etoile;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class MainTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private SparkSession session;

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
        final File output = temp.newFolder("output")
                .toPath()
                .resolve("csv")
                .toFile();
        Files.copy(
                Paths.get(
                        this.getClass()
                                .getClassLoader()
                                .getResource("test.avro")
                                .getFile()
                ),
                input.toPath()
                        .resolve("test.avro")
        );
        new Main(
                session,
                new Args(
                        "--path=" + input,
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

    @Test
    public void canSetAColumnSep() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.getRoot()
                .toPath()
                .resolve("output")
                .toFile();
        Files.copy(
                Paths.get(
                        this.getClass()
                                .getClassLoader()
                                .getResource("test.avro")
                                .getFile()
                ),
                input.toPath()
                        .resolve("test.avro")
        );
        new Main(
                session,
                new Args(
                        "--path=" + input,
                        "--output=" + output,
                        "--delimiter=X"
                )
        ).run();

        final List<File> files = Arrays
                .stream(output.listFiles((dir, name) -> name.endsWith("csv")))
                .collect(Collectors.toList());
        MatcherAssert.assertThat(
                "files were written",
                files,
                Matchers.hasSize(Matchers.greaterThan(0))
        );

        final List<String> lines = new ArrayList<>();
        for (final File csv : files) {
            lines.addAll(IOUtils.readLines(new FileReader(csv)));
        }
        MatcherAssert.assertThat(
                "each line contains delimiter",
                lines,
                Matchers.everyItem(
                        Matchers.containsString("X")
                )
        );
    }

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[1]")
                .getOrCreate();
    }
}
