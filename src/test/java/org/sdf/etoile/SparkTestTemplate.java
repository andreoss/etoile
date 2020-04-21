/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.TemporaryFolder;

/**
 * Template for tests with Spark.
 *
 * @since 0.1.0
 * @todo Replace with @{link Extension}
 */
@EnableRuleMigrationSupport
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
abstract class SparkTestTemplate {
    /**
     * Temporary folder.
     */
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    /**
     * The spark session.
     * @checkstyle VisibilityModifierCheck (3 lines)
     */
    protected SparkSession session;

    /**
     * Start session.
     *
     * @throws IOException If unable to create temp folder.
     */
    @BeforeEach
    void setUp() throws IOException {
        this.session = SparkSession.builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                StaticSQLConf.WAREHOUSE_PATH().key(),
                this.temp.newFolder().toString()
            )
            .enableHiveSupport()
            .master("local[*]")
            .getOrCreate();
    }

}
