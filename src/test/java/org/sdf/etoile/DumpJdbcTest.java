/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.h2.Driver;
import org.h2.tools.RunScript;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

/**
 * A test for {@link Dump} with JDBC source.
 *
 * @since 0.5.0
 */
@EnableRuleMigrationSupport
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public final class DumpJdbcTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @SuppressWarnings("UseOfJDBCDriverClass")
    @BeforeEach
    void setUpDatabase() throws SQLException {
        DriverManager.registerDriver(new Driver());
    }

    @Test
    void dumpsJdbcTableNameWithDollarSign() throws Exception {
        final String url = this.database(
            "CREATE TABLE `X#X$X`(`$id` int, `$name` varchar)",
            "INSERT INTO `X#X$X` VALUES(1, 'mr hands')",
            "INSERT INTO `X#X$X` VALUES(2, 'goes to farm')"
        );
        final File output = this.data.output();
        new Dump(
            this.session,
            new Args(
                "--input.format=jdbc",
                String.format("--input.url=%s", url),
                "--input.dbtable=`X#X$X`",
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
                "_ID,_NAME",
                "1,mr hands",
                "2,goes to farm"
            )
        );
    }

    @Test
    void dumpsJdbcTable() throws Exception {
        final String url = this.database(
            "CREATE TABLE BAR(id int, name varchar)",
            "INSERT INTO BAR VALUES(1, 'horse')",
            "INSERT INTO BAR VALUES(2, 'lover')"
        );
        final File output = this.data.output();
        new Dump(
            this.session,
            new Args(
                "--input.format=jdbc",
                String.format("--input.url=%s", url),
                "--input.dbtable=BAR",
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
                "ID,NAME",
                "1,horse",
                "2,lover"
            )
        );
    }

    private String database(final String... sql) throws IOException, SQLException {
        final String url = String.format("jdbc:h2:file:%s", temp.newFile("db.sql"));
        try (Connection conn = DriverManager.getConnection(url)) {
            final String command = Arrays
                .stream(sql)
                .collect(Collectors.joining(";"));
            try (ResultSet res = RunScript.execute(conn, new StringReader(command))
            ) {
                return url;
            }
        }
    }
}
