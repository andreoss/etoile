/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Second test for {@link Main}.
 * @since 0.4.0
 */
final class MainSecondTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

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
        new Main(
            this.session,
            new Args(
                "--input.format=csv",
                "--input.header=true",
                String.format("--input.path=%s", this.data.input().toURI()),
                String.format("--output.path=%s", output.toURI()),
                "--output.rename=id as num, name as nm",
                "--output.header=true",
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "columns should be renamed",
            new CsvText(this.data.output()),
            new LinesAre(
                "num,nm",
                lines[0],
                lines[1]
            )
        );
    }
}

