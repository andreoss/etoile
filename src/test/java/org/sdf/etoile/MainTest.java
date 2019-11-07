/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test {@link Main}.
 * @todo Refactor to sepearate test cases.
 * @checkstyle StringLiteralsConcatenationCheck (2000 lines)
 * @since 0.0.1
 */
@SuppressWarnings({
    "PMD.TooManyMethods",
    "PMD.AvoidDuplicateLiterals",
    "PMD.ExcessiveClassLength",
    "unchecked"
})
public final class MainTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @Test
    void requiresArguments() {
        Assertions.assertThrows(
            NullPointerException.class,
            new Main(
                Mockito.mock(SparkSession.class),
                Collections.emptyMap()
            )::run
        );
    }

    @Test
    void startsSpark() throws IOException {
        final File input = this.data.input();
        final File output = this.data.output();
        this.data.copyResource("test.avro");
        new Main(
            this.session,
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
    void canSetAColumnSep() throws IOException {
        final File input = this.data.input();
        final File output = this.data.output();
        this.data.copyResource("test.avro");
        new Main(
            this.session,
            new Args(
                "--input.path=" + input.toURI(),
                "--output.path=" + output.toURI(),
                "--output.delimiter=X"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        MatcherAssert.assertThat(
            "each line contains delimiter",
            this.data.outputLines(),
            Matchers.everyItem(Matchers.containsString("X"))
        );
    }

    @Test
    void canSpecifyNumberOfPartitions() throws IOException {
        this.data.writeInput(
            "a,b,c,d,e",
            "1,x,a,y,5",
            "2,x,b,y,5",
            "3,x,c,y,5",
            "4,x,d,y,5",
            "5,x,c,y,5"
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.delimiter=,",
                "--input.path=" + this.data.input().toURI(),
                "--output.partitions=1",
                "--output.path=" + this.data.output().toURI()
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.equalTo(1))
        );
    }

    @Test
    void canUseCustomFormat() throws IOException {
        this.data.writeInput(
            "1,2,3,4,5"
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.delimiter=,",
                "--input.path=" + this.data.input().toURI(),
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv",
                "--output.delimiter=#"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = this.data.outputLines();
        MatcherAssert.assertThat(
            "contains 1 line and delimiter is ;",
            lines,
            Matchers.contains(
                Matchers.is("1#2#3#4#5")
            )
        );
    }

    @Test
    void writesSortedOutput() throws IOException {
        this.data.copyResource("unsorted.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=CTL_SEQNO",
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        MatcherAssert.assertThat(
            "contains 3 lines sorted by CTL_SEQNO",
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.endsWith("I,1,1"),
                Matchers.endsWith("I,2,2"),
                Matchers.endsWith("I,3,3")
            )
        );
    }

    @Test
    void canCastTypesStringToTimestampTwoColumns() throws IOException {
        this.data.writeInput(
            "id,ts,ts",
            "0,2000-06-13 13:31:59,2019-06-13 13:31:59",
            "1,1999-06-13 13:31:59,2019-06-13 13:31:59"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=ts1",
                "--input.cast=ts1:timestamp,ts2:timestamp",
                "--output.path=" + output.toURI(),
                "--output.format=csv",
                "--output.delimiter=|"
            )
        ).run();
        MatcherAssert.assertThat(
            "converts two timestamp columns and sorts",
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("1|1999"),
                Matchers.startsWith("0|2000")
            )
        );
    }

    @Test
    void canCastTypeStringToTimestamp() throws IOException {
        this.data.writeInput(
            "id,ctl_validfrom,name",
            "0,2019-06-13 13:31:59,abc",
            "1,2019-06-13 13:31:59,xyz"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=id",
                "--input.cast=ctl_validfrom:timestamp",
                "--output.path=" + output.toURI(),
                "--output.format=csv",
                "--output.delimiter=|"
            )
        ).run();
        MatcherAssert.assertThat(
            "converts timestamp to string",
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("0|2019-06-13T13:31:59.000Z|abc"),
                Matchers.startsWith("1|2019-06-13T13:31:59.000Z|xyz")
            )
        );
    }

    @Test
    void canCastTypAllTimestampsToStringOnWrite() throws IOException {
        this.data.writeInput(
            "id,ctl_validfrom,name",
            "0,2019-06-13 13:31:59,abc",
            "1,2019-06-13 13:31:59,xyz"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
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
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("0|2019-06-13 13:31:59|abc"),
                Matchers.startsWith("1|2019-06-13 13:31:59|xyz")
            )
        );
    }

    @Test
    void canCastAllIntsToTimestampOnRead() throws IOException {
        this.data.writeInput(
            "id,ctl_validfrom,name",
            "0,0,abc",
            "1,0,xyz"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
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
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("0|1970-01-01 00"),
                Matchers.startsWith("1|1970-01-01 00")
            )
        );
    }

    @Test
    void canCastTypeStringToTimestampandTimestampToString() throws IOException {
        this.data.writeInput(
            "id,ctl_validfrom,name",
            "0,2019-06-13 13:31:59,abc",
            "1,2019-06-13 13:31:59,xyz"
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=id",
                "--input.cast=ctl_validfrom:timestamp",
                "--output.cast=ctl_validfrom:string",
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv",
                "--output.delimiter=|"
            )
        ).run();
        MatcherAssert.assertThat(
            "converts timestamp to string and back to timestamp",
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("0|2019-06-13 13:31:59|abc"),
                Matchers.startsWith("1|2019-06-13 13:31:59|xyz")
            )
        );
    }

    @Test
    void canCastTypeStringToIntandIntToTimestamp() throws IOException {
        this.data.writeInput(
            "id,ts",
            "0,0",
            "1,1000"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=id",
                "--input.cast=ts:int,ts:timestamp",
                "--output.path=" + output.toURI(),
                "--output.format=csv",
                "--output.delimiter=|"
            )
        ).run();
        MatcherAssert.assertThat(
            "converts timestamp to string and back to timestamp",
            this.data.outputLines(),
            IsIterableContainingInOrder.contains(
                Matchers.startsWith("0|1970-01-01T00:00:00.000Z"),
                Matchers.startsWith("1|1970-01-01T00:16:40.000Z")
            )
        );
    }

    @Test
    void writesSortedOutputCastsByColumnName() throws IOException {
        this.data.copyResource("unsorted.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + this.data.input().toURI(),
                "--input.cast=numb1:int",
                "--input.sort=NUMB1",
                "--output.path=" + this.data.output().toURI(),
                "--output.delimiter=|",
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = this.data.outputLines();
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
    void writesSortedOutputAndSortsNumerically() throws IOException {
        this.data.copyResource("unsorted.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=cast(NUMB1 as int)",
                "--output.path=" + this.data.output().toURI(),
                "--output.delimiter=|",
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = this.data.outputLines();
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
    void sortsInDescOrder() throws IOException {
        this.data.copyResource("unsorted.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + this.data.input().toURI(),
                "--input.sort=cast(NUMB1 as int):desc",
                "--output.path=" + this.data.output().toURI(),
                "--output.delimiter=|",
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "files were written",
            this.data.outputFiles(),
            Matchers.hasSize(Matchers.greaterThan(0))
        );
        final List<String> lines = this.data.outputLines();
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

    @Test
    void canSortBySeveralColumns() throws IOException {
        this.data.writeInput(
            "id,val,char",
            "1,3,o",
            "2,2,g",
            "2,1,u",
            "0,0,y",
            "3,4,y",
            "3,0,a"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.cast=id:int,val:int",
                "--input.sort=id,val",
                "--output.path=" + output.toURI(),
                "--output.format=csv",
                "--output.delimiter=;"
            )
        ).run();
        MatcherAssert.assertThat(
            "output sorted by id & val",
            this.data.outputLines(),
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

    @Test
    void doesNotKeepHeaderInOutput() throws IOException {
        this.data.writeInput("id,val,char");
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
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
            this.data.outputLines(),
            Matchers.empty()
        );
    }

    @Test
    void convertsEmptyFileWithoutHeader() throws IOException {
        this.data.writeInput();
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.path=" + this.data.input().toURI(),
                "--output.path=" + output.toURI()
            )
        ).run();
        MatcherAssert.assertThat(
            "no files written",
            this.data.outputFiles(),
            Matchers.empty()
        );
    }

    @Test
    void convertsEmptyFileWithHeaderRequired() throws IOException {
        this.data.writeInput();
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.header=true",
                "--output.header=true",
                "--input.format=csv",
                "--input.path=" + this.data.input().toURI(),
                "--output.path=" + output.toURI()
            )
        ).run();
        MatcherAssert.assertThat(
            "no files written",
            this.data.outputFiles(),
            Matchers.empty()
        );
    }

    @Test
    void convertsEmptyCsvWithHeaderToEmptyFile() throws IOException {
        this.data.writeInput("id,name,value");
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.header=true",
                "--output.header=true",
                "--input.format=csv",
                "--input.path=" + this.data.input().toURI(),
                "--output.path=" + output.toURI()
            )
        ).run();
        MatcherAssert.assertThat(
            "one file written",
            this.data.outputFiles(),
            Matchers.hasSize(1)
        );
        MatcherAssert.assertThat(
            "empty output",
            this.data.outputLines(),
            Matchers.empty()
        );
    }

    @Test
    void doesNotKeepHeaderInOutputWhenMultipleFilesAllEmpty() throws IOException {
        final File input = new TempFiles(this.temp).input();
        final File first = input.toPath().resolve("part-1").toFile();
        final File second = input.toPath().resolve("part-2").toFile();
        Assertions.assertTrue(first.mkdirs());
        Assertions.assertTrue(second.mkdirs());
        Assertions.assertTrue(first.toPath().resolve("1.csv").toFile().createNewFile());
        Assertions.assertTrue(second.toPath().resolve("1.csv").toFile().createNewFile());
        final File output = this.data.output();
        new Main(
            this.session,
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
            this.data.outputFiles(),
            Matchers.empty()
        );
    }

    @Test
    void doesNotKeepHeaderInOutputMultipleFiles() throws IOException {
        final File input = this.data.input();
        this.data.writeInput("id,val,char");
        this.data.writeInput("id,val,char");
        final File output = this.data.output();
        new Main(
            this.session,
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
            this.data.outputLines(),
            Matchers.empty()
        );
    }

    @Test
    void canDropColumnFromOutput() throws IOException {
        this.data.writeInput(
            "id,val,name",
            "0,2,go",
            "1,1,at",
            "2,0,se"
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--output.drop=name",
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "output sorted by id & val",
            this.data.outputLines(),
            Matchers.contains(
                Matchers.is("0,2"),
                Matchers.is("1,1"),
                Matchers.is("2,0")
            )
        );
    }

    @Test
    void writesHeaderForSpecialFormatWhenNonEmpty() throws IOException {
        this.data.writeInput(
            "id,val,num",
            "1,foo,1234.0",
            "0,bar,0.1234",
            "2,baz,12.34"
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--input.csv=num:decimal(38,12)",
                "--input.sort=num",
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv+header",
                "--output.delimiter=;"
            )
        ).run();
        MatcherAssert.assertThat(
            "keeps header",
            new CsvText(this.data.output()),
            new LinesAre(
                "id;val;num",
                "0;bar;0.1234",
                "2;baz;12.34",
                "1;foo;1234.0"
            )
        );
    }

    @Test
    void writesHeaderForSpecialFormat() throws IOException {
        this.data.writeInput("id,val,char");
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
                "--output.path=" + this.data.output().toURI(),
                "--output.format=csv+header",
                "--output.delimiter=;"
            )
        ).run();
        MatcherAssert.assertThat(
            "header discarded on write",
            new CsvText(this.data.output()),
            new LinesAre(
                "id;val;char"
            )
        );
    }

    @Test
    void overwritesPreviousOutput() throws IOException {
        this.data.writeInput("id,val,char");
        final File output = this.data.output();
        Assertions.assertTrue(output.mkdirs());
        Assertions.assertTrue(
            output.toPath().resolve("foo.csv").toFile().createNewFile()
        );
        Assertions.assertTrue(
            output.toPath().resolve("bar.csv").toFile().createNewFile()
        );
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + this.data.input().toURI(),
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

    @Test
    void appendsToPreviousOutput() {
        final TempFiles second = new TempFiles(this.temp);
        final List<File> inputs =
            Arrays.asList(
                second.input(),
                this.data.input()
            );
        this.data.writeInput(
            "id,val,char",
            "1,foo,bar",
            "3,abc,def"
        );
        second.writeInput(
            "id,val,char",
            "2,baz,gii",
            "4,xyz,qwe"
        );
        final File output = this.data.output();
        for (final File input : inputs) {
            new Main(
                this.session,
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
        final File aggreg = second.output();
        new Main(
            this.session,
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
    void canReplacesValuesByType() throws IOException {
        final File input = this.data.input();
        this.data.writeInput(
            "id,val,char",
            "1,XXX,abc"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + input.toURI(),
                "--output.replace=string:XXX/MISSING",
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

    @Test
    void handlesColumnExprs() throws IOException {
        final File input = this.data.input();
        this.data.writeInput(
            "id\tval\tnum",
            "1\tfoo\t1234.6"
        );
        final File output = this.data.output();
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                "--input.path=" + input.toURI(),
                "--input.delimiter=\t",
                "--input.csv=num:decimal(38,12)",
                "--input.expr=num:ceil(num)",
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
                "1;foo;1235"
            )
        );
    }

    @Test
    void handlesExpressionsWithAvro() throws IOException {
        final File input = this.data.input();
        final File output = this.data.output();
        this.data.copyResource("missing.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + input.toURI(),
                "--input.expr="
                    + "type_number:missing(type_number),"
                    + "type_timestamp:missing(type_timestamp)",
                "--output.path=" + output.toURI(),
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "header discarded on write",
            new CsvText(output),
            new LinesAre(
                "1,5321312.12466,MISSING",
                "2,MISSING,MISSING",
                "3,9921312.13499,2011-10-17 23:11:12.000000",
                "4,3321312.13499,2011-11-17 23:11:12.000000",
                "5,4421312.13499,2011-12-17 23:11:12.000000"
            )
        );
    }

    @Test
    void handlesExpressionsWithSpecialFormat() throws IOException {
        final File input = this.data.input();
        final File output = this.data.output();
        this.data.copyResource("missing.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=avro+missing",
                "--input.path=" + input.toURI(),
                "--output.path=" + output.toURI(),
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "header discarded on write",
            new CsvText(output),
            new LinesAre(
                "1,5321312.12466,MISSING",
                "2,MISSING,MISSING",
                "3,9921312.13499,2011-10-17 23:11:12.000000",
                "4,3321312.13499,2011-11-17 23:11:12.000000",
                "5,4421312.13499,2011-12-17 23:11:12.000000"
            )
        );
    }

    @Test
    void replacesMissingValues() throws IOException {
        final File input = this.data.input();
        final File output = this.data.output();
        this.data.copyResource("missing.avro");
        new Main(
            this.session,
            new Args(
                "--input.format=com.databricks.spark.avro",
                "--input.path=" + input.toURI(),
                "--output.replace=string:\u0001/MISSING",
                "--output.path=" + output.toURI(),
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "header discarded on write",
            new CsvText(output),
            new LinesAre(
                "1,5321312.12466,MISSING",
                "2,MISSING,MISSING",
                "3,9921312.13499,2011-10-17 23:11:12.000000",
                "4,3321312.13499,2011-11-17 23:11:12.000000",
                "5,4421312.13499,2011-12-17 23:11:12.000000"
            )
        );
    }

}

