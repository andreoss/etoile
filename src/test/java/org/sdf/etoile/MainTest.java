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

import java.io.*;
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
        copyAvro(input, "test.avro");
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
        copyAvro(input, "test.avro");
        new Main(
                session,
                new Args(
                        "--input.path=" + input,
                        "--output.path=" + output,
                        "--output.delimiter=X"
                )
        ).run();
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.greaterThan(0))
        );

        MatcherAssert.assertThat(
                "each line contains delimiter",
                readAllLines(output),
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
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.equalTo(1))
        );
    }

    private List<File> listCsvFiles(final File output) {
        return Arrays
                .stream(output.listFiles((dir, name) -> name.endsWith("csv")))
                .collect(Collectors.toList());
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
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = readAllLines(output);
        MatcherAssert.assertThat(
                "contains 1 line and delimiter is ;",
                lines,
                Matchers.contains(
                        Matchers.is("1#2#3#4#5")
                )
        );
    }

    private List<String> readAllLines(final List<File> files) throws IOException {
        final List<String> lines = new ArrayList<>();
        for (final File csv : files) {
            lines.addAll(IOUtils.readLines(new FileReader(csv)));
        }
        return lines;
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
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.greaterThan(0))
        );
        MatcherAssert.assertThat(
                "contains 3 lines sorted by CTL_SEQNO",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.endsWith("I,1,1"),
                        Matchers.endsWith("I,2,2"),
                        Matchers.endsWith("I,3,3")
                )
        );
    }

    @Test
    public void canCastType_StringToTimestamp_TwoColumns() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input, "id,ts,ts", "0,2000-06-13 13:31:59,2019-06-13 13:31:59", "1,1999-06-13 13:31:59,2019-06-13 13:31:59");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=ts1",
                        "--input.cast=ts1:timestamp,ts2:timestamp",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts two timestamp columns and sorts",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("1|1999"),
                        Matchers.startsWith("0|2000")
                )
        );
    }

    @Test
    public void canCastType_StringToTimestamp() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input, "id,ctl_validfrom,name", "0,2019-06-13 13:31:59,abc", "1,2019-06-13 13:31:59,xyz");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts timestamp to string",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("0|2019-06-13T13:31:59.000Z|abc"),
                        Matchers.startsWith("1|2019-06-13T13:31:59.000Z|xyz")
                )
        );
    }

    @Test
    public void canCastTypAllTimestampsToStringOnWrite() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input, "id,ctl_validfrom,name", "0,2019-06-13 13:31:59,abc", "1,2019-06-13 13:31:59,xyz");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.convert=timestamp:string",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts timestamp to string and back to timestamp",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("0|2019-06-13 13:31:59|abc"),
                        Matchers.startsWith("1|2019-06-13 13:31:59|xyz")
                )
        );
    }

    @Test
    public void canCastAllIntsToTimestampOnRead() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input,
                "id,ctl_validfrom,name",
                "0,0,abc",
                "1,0,xyz");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:int,ctl_validfrom:timestamp",
                        "--input.convert=timestamp:string",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts timestamp to string and back to timestamp",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("0|1970-01-01 00"),
                        Matchers.startsWith("1|1970-01-01 00")
                )
        );
    }

    private File writeCsv(final File input, final String... lines) throws IOException {
        final File output = temp.newFolder("output")
                .toPath()
                .resolve("csv")
                .toFile();
        IOUtil.writeText(
                String.join("\n",
                        lines
                ),
                input.toPath().resolve("test-input.csv").toFile()
        );
        return output;
    }

    @Test
    public void canCastType_StringToTimestamp_andTimestampToString() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input, "id,ctl_validfrom,name", "0,2019-06-13 13:31:59,abc", "1,2019-06-13 13:31:59,xyz");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.cast=ctl_validfrom:string",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts timestamp to string and back to timestamp",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("0|2019-06-13 13:31:59|abc"),
                        Matchers.startsWith("1|2019-06-13 13:31:59|xyz")
                )
        );
    }

    @Test
    public void canCastType_StringToInt_andIntToTimestamp() throws IOException {
        final File input = temp.newFolder("input");
        final File output = writeCsv(input,
                "id,ts",
                "0,0",
                "1,1000"
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input,
                        "--input.sort=id",
                        "--input.cast=ts:int,ts:timestamp",
                        "--output.path=" + output,
                        "--output.format=csv",
                        "--output.delimiter=|"
                )
        ).run();
        MatcherAssert.assertThat(
                "converts timestamp to string and back to timestamp",
                readAllLines(output),
                IsIterableContainingInOrder.contains(
                        Matchers.startsWith("0|1970-01-01T00:00:00.000Z"),
                        Matchers.startsWith("1|1970-01-01T00:16:40.000Z")
                )
        );
    }

    private List<String> readAllLines(final File output) throws IOException {
        return readAllLines(listCsvFiles(output));
    }

    @Test
    public void writesSortedOutput_castsByColumnName() throws IOException {
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
                        "--input.cast=numb1:int",
                        "--input.sort=NUMB1",
                        "--output.path=" + output,
                        "--output.delimiter=|",
                        "--output.format=csv"
                )
        ).run();
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = readAllLines(output);
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
        MatcherAssert.assertThat(
                "files were written",
                listCsvFiles(output),
                Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = readAllLines(output);
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
        final File copy = Paths.get(input.getAbsolutePath(), name).toFile();
        try (final InputStream stream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(name);
             final OutputStream output = new FileOutputStream(copy)
        ) {
            IOUtils.copy(stream, output);
        }
    }


    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }
}