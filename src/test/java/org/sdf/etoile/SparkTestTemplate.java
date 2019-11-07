/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import org.apache.spark.sql.SparkSession;
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
     */
    @BeforeEach
    void setUp() {
        this.session = SparkSession.builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[*]")
            .getOrCreate();
    }

}
