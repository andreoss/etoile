/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

/**
 * Third test for {@link Dump}.
 * @since 0.4.0
 */
final class DumpThirdTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @Test
    void canDumpHiveTable() {
        SparkTestTemplate.session.sql(
            "CREATE DATABASE IF NOT EXISTS FOO"
        ).count();
        SparkTestTemplate.session.sql(
            "CREATE TABLE IF NOT EXISTS FOO.BAR AS (SELECT 1 as ID, 'foo' as NAME)"
        ).count();
        final File output = this.data.output();
        new Dump(
            SparkTestTemplate.session,
            new Args(
                "--input.table=foo.bar",
                String.format("--output.path=%s", output.toURI()),
                "--output.hive-names=true",
                "--output.header=true",
                "--output.format=csv"
            )
        ).run();
        MatcherAssert.assertThat(
            "should write data from hive table",
            new CsvText(this.data.output()),
            new LinesAre(
                "ID,NAME",
                "1,foo"
            )
        );
    }

}
