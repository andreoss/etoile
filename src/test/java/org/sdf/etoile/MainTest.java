package org.sdf.etoile;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.internal.util.io.IOUtil;

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
        copyAvro(input);
        new Main(
                session,
                new Args(
                        "--input.path=" + input,
                        "--output.path=" + output
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
        copyAvro(input);
        new Main(
                session,
                new Args(
                        "--input.path=" + input,
                        "--output.path=" + output,
                        "--output.delimiter=X"
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

    @Test
    public void canSpecifyNumberOfPartitions() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.getRoot()
                .toPath()
                .resolve("output")
                .toFile();
        IOUtil.writeText(
                String.join("\n",
                        "a,b,c,d,e",
                        "1,x,a,y,5",
                        "2,x,b,y,5",
                        "3,x,c,y,5",
                        "4,x,d,y,5",
                        "5,x,c,y,5"
                ),
                input.toPath().resolve("test-input.csv").toFile()
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.delimiter=,",
                        "--input.path=" + input,
                        "--output.partitions=1",
                        "--output.path=" + output
                )
        ).run();
        final List<File> files = Arrays
                .stream(output.listFiles((dir, name) -> name.endsWith("csv")))
                .collect(Collectors.toList());
        MatcherAssert.assertThat(
                "files were written",
                files,
                Matchers.hasSize(Matchers.equalTo(1))
        );
    }


    @Test
    public void canUseCustomFormat() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.getRoot()
                .toPath()
                .resolve("output")
                .toFile();
        IOUtil.writeText(
                "1,2,3,4,5",
                input.toPath().resolve("test-input.csv").toFile()
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.delimiter=,",
                        "--input.path=" + input,
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=#"
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
                "contains 1 line and delimiter is ;",
                lines,
                Matchers.contains(
                        Matchers.is("1#2#3#4#5")
                )
        );
    }

    @Test
    public void writesSortedOutput() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.newFolder("output")
                .toPath()
                .resolve("csv")
                .toFile();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input,
                        "--input.sort=CTL_SEQNO",
                        "--output.path=" + output,
                        "--output.format=csv"
                )
        ).run();
        final List<File> files = Arrays
                .stream(output.listFiles((dir, name) -> name.endsWith("csv")))
                .sorted()
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
                "contains 3 lines sorted by CTL_SEQNO",
                lines,
                IsIterableContainingInOrder.contains(
                        Matchers.endsWith("I,1,1"),
                        Matchers.endsWith("I,2,2"),
                        Matchers.endsWith("I,3,3")
                )
        );
    }

    @Test
    public void writesSortedOutput_AndSortsNumerically() throws IOException {
        final File input = temp.newFolder("input");
        final File output = temp.newFolder("output")
                .toPath()
                .resolve("csv")
                .toFile();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input,
                        "--input.sort=cast(NUMB1 as int)",
                        "--output.path=" + output,
                        "--output.delimiter=|",
                        "--output.format=csv"
                )
        ).run();
        final List<File> files = Arrays
                .stream(output.listFiles((dir, name) -> name.endsWith("csv")))
                .sorted()
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
                "contains 3 lines sorted by NUMB1",
                lines,
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("wrwrwrw|4|3"),
                        Matchers.startsWith("wwer|5|12"),
                        Matchers.startsWith("RRE|3|55")
                )
        );
    }

    private void copyAvro(final File input, final String name) throws IOException {
        Files.copy(
                Paths.get(
                        this.getClass()
                                .getClassLoader()
                                .getResource(name)
                                .getFile()
                ),
                input.toPath()
                        .resolve(name)
        );
    }


    private void copyAvro(final File input) throws IOException {
        copyAvro(input, "test.avro");
    }

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }
}