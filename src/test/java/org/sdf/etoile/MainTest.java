package org.sdf.etoile;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.internal.util.io.IOUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public final class MainTest extends SparkTestTemplate {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

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
        final File output = resolveCsvOutput();
        copyAvro(input, "test.avro");
        new Main(
                session,
                new Args(
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI()
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
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "a,b,c,d,e",
                "1,x,a,y,5",
                "2,x,b,y,5",
                "3,x,c,y,5",
                "4,x,d,y,5",
                "5,x,c,y,5"
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.delimiter=,",
                        "--input.path=" + input.toURI(),
                        "--output.partitions=1",
                        "--output.path=" + output.toURI()
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
                .stream(Objects.requireNonNull(output.listFiles((dir, name) -> name.endsWith("csv"))))
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
                input.toPath()
                        .resolve("test-input.csv")
                        .toFile()
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.delimiter=,",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI(),
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
        final File output = resolveCsvOutput();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input.toURI(),
                        "--input.sort=CTL_SEQNO",
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "id,ts,ts",
                "0,2000-06-13 13:31:59,2019-06-13 13:31:59",
                "1,1999-06-13 13:31:59,2019-06-13 13:31:59"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=ts1",
                        "--input.cast=ts1:timestamp,ts2:timestamp",
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "id,ctl_validfrom,name",
                "0,2019-06-13 13:31:59,abc",
                "1,2019-06-13 13:31:59,xyz"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "id,ctl_validfrom,name",
                "0,2019-06-13 13:31:59,abc",
                "1,2019-06-13 13:31:59,xyz"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.convert=timestamp:string",
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "id,ctl_validfrom,name",
                "0,0,abc",
                "1,0,xyz"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:int,ctl_validfrom:timestamp",
                        "--input.convert=timestamp:string",
                        "--output.path=" + output.toURI(),
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

    private void writeInputFile(final File input, final String... lines) {
        IOUtil.writeText(
                String.join("\n", lines),
                input.toPath()
                        .resolve("test-input.csv")
                        .toFile()
        );
    }

    @Test
    public void canCastType_StringToTimestamp_andTimestampToString() throws IOException {
        final File input = temp.newFolder("input");
        final File output = resolveCsvOutput();
        writeInputFile(input,
                "id,ctl_validfrom,name",
                "0,2019-06-13 13:31:59,abc",
                "1,2019-06-13 13:31:59,xyz"
        );
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=id",
                        "--input.cast=ctl_validfrom:timestamp",
                        "--output.cast=ctl_validfrom:string",
                        "--output.path=" + output.toURI(),
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
        writeInputFile(input,
                "id,ts",
                "0,0",
                "1,1000"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.sort=id",
                        "--input.cast=ts:int,ts:timestamp",
                        "--output.path=" + output.toURI(),
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
        final File output = resolveCsvOutput();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input.toURI(),
                        "--input.cast=numb1:int",
                        "--input.sort=NUMB1",
                        "--output.path=" + output.toURI(),
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
        final File output = resolveCsvOutput();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input.toURI(),
                        "--input.sort=cast(NUMB1 as int)",
                        "--output.path=" + output.toURI(),
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
    public void sortsInDescOrder() throws IOException {
        final File input = temp.newFolder("input");
        final File output = resolveCsvOutput();
        copyAvro(input, "unsorted.avro");
        new Main(
                session,
                new Args(
                        "--input.format=com.databricks.spark.avro",
                        "--input.path=" + input.toURI(),
                        "--input.sort=cast(NUMB1 as int):desc",
                        "--output.path=" + output.toURI(),
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
                        Matchers.startsWith("RRE|3|55"),
                        Matchers.startsWith("wwer|5|12"),
                        Matchers.startsWith("wrwrwrw|4|3")
                )
        );
    }


    private void copyAvro(final File input, final String name) throws IOException {
        final File copy = Paths.get(input.getAbsolutePath(), name)
                .toFile();
        try (final InputStream stream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(name);
             final OutputStream output = new FileOutputStream(copy)
        ) {
            IOUtils.copy(Objects.requireNonNull(stream), output);
        }
    }

    @Test
    public void canSortBySeveralColumns() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,char",
                "1,3,o",
                "2,2,g",
                "2,1,u",
                "0,0,y",
                "3,4,y",
                "3,0,a"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.cast=id:int,val:int",
                        "--input.sort=id,val",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv",
                        "--output.delimiter=;"
                )
        ).run();
        MatcherAssert.assertThat(
                "output sorted by id & val",
                readAllLines(output),
                Matchers.contains(
                        Matchers.is("0;0;y"),
                        Matchers.is("1;3;o"),
                        Matchers.is("2;1;u"),
                        Matchers.is("2;2;g"),
                        Matchers.is("3;0;a"),
                        Matchers.is("3;4;y")
                )
        );
    }

    private File resolveCsvOutput(final String dir) throws IOException {
        return temp.newFolder()
                .toPath()
                .resolve(dir)
                .toFile();
    }

    private File resolveCsvOutput() throws IOException {
        return resolveCsvOutput("csv");
    }

    @Test
    public void doesNotKeepHeaderInOutput() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,char");
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.cast=id:int,val:int",
                        "--input.sort=id,val",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv",
                        "--output.delimiter=;",
                        "--output.header=true"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                readAllLines(output),
                Matchers.empty()
        );
    }


    @Test
    public void convertsEmptyFileWithoutHeader() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input);
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI()
                )
        ).run();
        MatcherAssert.assertThat(
                "no files written",
                listCsvFiles(output),
                Matchers.empty()
        );
    }

    @Test
    public void convertsEmptyFileWithHeaderRequired() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input);
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.header=true",
                        "--output.header=true",
                        "--input.format=csv",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI()
                )
        ).run();
        MatcherAssert.assertThat(
                "no files written",
                listCsvFiles(output),
                Matchers.empty()
        );
    }

    @Test
    public void convertsEmptyCsvWithHeaderToEmptyFile() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,name,value");
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.header=true",
                        "--output.header=true",
                        "--input.format=csv",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI()
                )
        ).run();
        MatcherAssert.assertThat(
                "one file written",
                listCsvFiles(output),
                Matchers.hasSize(1)
        );
        MatcherAssert.assertThat(
                "empty output",
                readAllLines(output),
                Matchers.empty()
        );
    }


    @Test
    public void doesNotKeepHeaderInOutput_MultipleFiles_AllEmpty() throws IOException {
        final File input = temp.newFolder("input");
        final File partA = input.toPath()
                .resolve("part-1")
                .toFile();
        final File partB = input.toPath()
                .resolve("part-2")
                .toFile();
        Assert.assertTrue(partA.mkdirs());
        Assert.assertTrue(partB.mkdirs());
        writeInputFile(partA);
        writeInputFile(partB);
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input + "/*",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv",
                        "--output.delimiter=;",
                        "--output.header=true"
                )
        ).run();
        MatcherAssert.assertThat(
                "no files written",
                listCsvFiles(output),
                Matchers.empty()
        );
    }

    @Test
    public void doesNotKeepHeaderInOutput_MultipleFiles() throws IOException {
        final File input = temp.newFolder("input");
        final File first = input.toPath()
                .resolve("part-1")
                .toFile();
        final File second = input.toPath()
                .resolve("part-2")
                .toFile();
        Assert.assertTrue(first.mkdirs());
        Assert.assertTrue(second.mkdirs());
        writeInputFile(first,
                "id,val,char"
        );
        writeInputFile(second,
                "id,val,char"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input + "/*",
                        "--input.cast=id:int,val:int",
                        "--input.sort=id,val",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv",
                        "--output.delimiter=;",
                        "--output.header=true"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                readAllLines(output),
                Matchers.empty()
        );
    }

    @Test
    public void canDropColumnFromOutput() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,name",
                "0,2,go",
                "1,1,at",
                "2,0,se"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--output.drop=name",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv"
                )
        ).run();
        MatcherAssert.assertThat(
                "output sorted by id & val",
                readAllLines(output),
                Matchers.contains(
                        Matchers.is("0,2"),
                        Matchers.is("1,1"),
                        Matchers.is("2,0")
                )
        );
    }

    @Test
    public void writesHeaderForSpecialFormat_NonEmpty() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,num",
                "1,foo,1234.0",
                "0,bar,0.1234",
                "2,baz,12.34"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.csv=num:decimal(38,12)",
                        "--input.sort=num",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv+header",
                        "--output.delimiter=;"
                )
        ).run();
        MatcherAssert.assertThat(
                "keeps header",
                new CsvText(output),
                new LinesAre(
                        "id;val;num",
                        "0;bar;0.1234",
                        "2;baz;12.34",
                        "1;foo;1234.0"
                )
        );
    }


    @Test
    public void writesHeaderForSpecialFormat() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,char"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI(),
                        "--output.format=csv+header",
                        "--output.delimiter=;"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                new CsvText(output),
                new LinesAre(
                        "id;val;char"
                )
        );
    }


    @Test
    public void overwritesPreviousOutput() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,char"
        );
        final File output = resolveCsvOutput();
        Assert.assertTrue("should exist", output.mkdirs());
        Assert.assertTrue("should exist", output.toPath()
                .resolve("foo.csv")
                .toFile()
                .createNewFile());
        Assert.assertTrue("should exist", output.toPath()
                .resolve("bar.csv")
                .toFile()
                .createNewFile());
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--output.path=" + output.toURI(),
                        "--output.format=csv+header",
                        "--output.delimiter=;",
                        "--output.mode=overwrite"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                new CsvText(output),
                new LinesAre(
                        "id;val;char"
                )
        );
    }

    public void appendsToPreviousOutput() throws IOException {
        final List<File> inputs =
                Arrays.asList(
                        temp.newFolder("input-part1"),
                        temp.newFolder("input-part2")
                );
        writeInputFile(inputs.get(0),
                "id,val,char",
                "1,foo,bar",
                "3,abc,def"
        );
        writeInputFile(inputs.get(1),
                "id,val,char",
                "2,baz,gii",
                "4,xyz,qwe"
        );
        final File output = resolveCsvOutput();
        for (final File input : inputs) {
            new Main(
                    session,
                    new Args(
                            "--input.format=csv",
                            "--input.header=true",
                            "--input.path=" + input.toURI(),
                            "--output.path=" + output.toURI(),
                            "--output.format=csv+header",
                            "--output.delimiter=;",
                            "--output.mode=append"
                    )
            ).run();
        }
        final File aggreg = resolveCsvOutput("agg");
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.delimiter=;",
                        "--input.path=" + output,
                        "--input.sort=id",
                        "--output.path=" + aggreg.toURI(),
                        "--output.format=csv+header",
                        "--output.mode=overwrite"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                new CsvText(aggreg),
                new LinesAre(
                        "id,val,char",
                        "1,foo,bar",
                        "2,baz,gii",
                        "3,abc,def",
                        "4,xyz,qwe"
                )
        );
    }

    @Test
    public void canReplacesValuesByType() throws IOException {
        final File input = temp.newFolder("input");
        writeInputFile(input,
                "id,val,char",
                "1,XXX,abc"
        );
        final File output = resolveCsvOutput();
        new Main(
                session,
                new Args(
                        "--input.format=csv",
                        "--input.header=true",
                        "--input.path=" + input.toURI(),
                        "--input.replace=string:XXX:MISSING",
                        "--output.path=" + output.toURI(),
                        "--output.format=csv+header",
                        "--output.delimiter=;"
                )
        ).run();
        MatcherAssert.assertThat(
                "header discarded on write",
                new CsvText(output),
                new LinesAre(
                        "id;val;char",
                        "1;MISSING;abc"
                )
        );
    }
}

