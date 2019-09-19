package org.sdf.etoile;

import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.TemporaryFolder;

@EnableRuleMigrationSupport
abstract class SparkTestTemplate {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    protected SparkSession session;

    @BeforeEach
    void setUp() {
        session = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[*]")
                .getOrCreate();
    }

}
