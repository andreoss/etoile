/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
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
@EnableRuleMigrationSupport
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
abstract class SparkTestTemplate {
    /**
     * The spark session.
     * @checkstyle VisibilityModifierCheck (3 lines)
     */
    protected static SparkSession session;

    /**
     * Temporary folder.
     */
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    /**
     * Start session.
     *
     * @throws IOException If unable to create temp folder.
     */
    @BeforeAll
    static void setUp() throws IOException {
        final TemporaryFolder temp = new TemporaryFolder();
        temp.create();
        final File scratch = temp.newFolder("scratch");
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

}
