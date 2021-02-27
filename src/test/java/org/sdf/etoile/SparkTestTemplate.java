/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.TemporaryFolder;

/**
 * Template for tests with Spark.
 *
 * @since 0.1.0
 * @todo #0:24h Consider turning this class to Extension. Note that
 *  it works on per-fork basis, saving some time in tests.
 *  The temporary directory is not used by this class also.
 */
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
@EnableRuleMigrationSupport
abstract class SparkTestTemplate {
    /**
     * The spark session.
     * @checkstyle VisibilityModifierCheck (3 lines)
     */
    protected static SparkSession session;

    /**
     * Temporary folder per session.
     */
    @ClassRule
    public static final TemporaryFolder DWH = new TemporaryFolder();

    /**
     * Temporary folder per test.
     */
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    /**
     * Files for tests.
     */
    protected final TestFiles data = new TempFiles(this.temp);

    /**
     * Start session.
     *
     * @throws IOException If unable to create temp folder.
     */
    @BeforeAll
    static void setUpAll() throws IOException {
        SparkTestTemplate.DWH.create();
        final File scratch = SparkTestTemplate.DWH.newFolder();
        scratch.setExecutable(true, false);
        scratch.setReadable(true, false);
        scratch.setWritable(true, false);
        System.setProperty("derby.system.home", scratch.toString());
        SparkTestTemplate.session = SparkSession.builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                StaticSQLConf.WAREHOUSE_PATH().key(),
                scratch.toString()
            )
            .config("hive.exec.scratchdir", scratch.toString())
            .config("hive.exec.local.scratchdir", scratch.toString())
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .enableHiveSupport()
            .master("local[*]")
            .getOrCreate();
    }

    /**
     * The Spark.
     * @return Session.
     */
    protected SparkSession session() {
        return SparkTestTemplate.session;
    }
}
