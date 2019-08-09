package org.sdf.etoile;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class SparkTestTemplate {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    protected SparkSession session;

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[*]")
                .getOrCreate();
    }

}
