/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Second test for {@link Dump}.
 * @since 0.4.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class DumpSecondTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @Test
    void canRenamesColumnsToHiveCompatableNams() {
        final String[] lines = {
            "0,abc",
            "1,xyz",
        };
        this.data.writeInput(
            "ID#ID,NAME$NAME",
            lines[0],
            lines[1]
        );
        final File output = this.data.output();
        new Dump(
            SparkTestTemplate.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                String.format("--input.path=%s", this.data.input().toURI()),
                String.format("--output.path=%s", output.toURI()),
                "--output.hive-names=true",
                "--output.header=true"
            )
        ).run();
        MatcherAssert.assertThat(
            "columns should be renamed",
            new CsvText(this.data.output()),
            new LinesAre(
                "ID_ID,NAME_NAME",
                lines[0],
                lines[1]
            )
        );
    }

    @Test
    void whenParameterSetThenShouldRenameColumns() {
        final String[] lines = {
            "0,foo",
            "1,bar",
        };
        this.data.writeInput(
            "id,name",
            lines[0],
            lines[1]
        );
        final File output = this.data.output();
        new Dump(
            SparkTestTemplate.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                String.format("--input.path=%s", this.data.input().toURI()),
                String.format("--output.path=%s", output.toURI()),
                "--output.rename=id as num, name as nm",
                "--output.header=true"
            )
        ).run();
        MatcherAssert.assertThat(
            "missing values should be repleaced",
            new CsvText(this.data.output()),
            new LinesAre(
                "num,nm",
                lines[0],
                lines[1]
            )
        );
    }

    @Test
    void handlesMissingFormatModifierWithCustomValues() {
        final String[] lines = {
            "id,name",
            "2,XXX",
            "3,baz",
        };
        this.data.writeInput(
            lines[0],
            lines[1],
            lines[2]
        );
        final File output = this.data.output();
        new Dump(
            SparkTestTemplate.session,
            new Args(
                "--input.format=csv+missing",
                "--input.header=true",
                "--input.missing=XXX",
                "--output.missing=YYY",
                String.format("--input.path=%s", this.data.input().toURI()),
                String.format("--output.path=%s", output.toURI()),
                "--output.header=true"
            )
        ).run();
        MatcherAssert.assertThat(
            "missing values should be replaced by specified values",
            new CsvText(this.data.output()),
            new LinesAre(
                lines[0],
                "2,YYY",
                lines[2]
            )
        );
    }

    @Test
    void handlesMissingFormatModifier() {
        final String[] lines = {
            "id,name",
            "0,\u0001",
            "1,bar",
        };
        this.data.writeInput(
            lines[0],
            lines[1],
            lines[2]
        );
        final File output = this.data.output();
        new Dump(
            SparkTestTemplate.session,
            new Args(
                "--input.format=csv+missing",
                "--input.header=true",
                String.format("--input.path=%s", this.data.input().toURI()),
                String.format("--output.path=%s", output.toURI()),
                "--output.header=true"
            )
        ).run();
        MatcherAssert.assertThat(
            "columns should be renamed",
            new CsvText(this.data.output()),
            new LinesAre(
                lines[0],
                "0,DEFAULT_VALUE",
                lines[2]
            )
        );
    }
}

