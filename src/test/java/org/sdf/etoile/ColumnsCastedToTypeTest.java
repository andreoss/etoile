package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ColumnsCastedToTypeTest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .getOrCreate();
    }

    @Test
    public void castsColumns() {
        final Dataset<Row> df = mkDataset();
        MatcherAssert.assertThat(
                "casts column",
                new TypeToColumnsMap(
                        new ColumnsCastedToType<>(
                                () -> df,
                                Collections.singletonMap(
                                        "valid", "timestamp"
                                )
                        )
                ),
                IsMapContaining.hasEntry(
                        Matchers.instanceOf(TimestampType.class),
                        Matchers.contains("valid")
                )
        );
    }

    @Test
    public void castsSeveralColumns() {
        final Dataset<Row> df = mkDataset();
        final Map<String, String> casts = new HashMap<>();
        casts.put("id", "int");
        casts.put("valid", "timestamp");
        MatcherAssert.assertThat(
                "casts column",
                new TypeToColumnsMap(
                        new ColumnsCastedToType<>(
                                () -> df,
                                casts
                        )
                ),
                Matchers.allOf(
                        IsMapContaining.hasEntry(
                                Matchers.instanceOf(IntegerType.class),
                                Matchers.contains("id")
                        ),
                        IsMapContaining.hasEntry(
                                Matchers.instanceOf(TimestampType.class),
                                Matchers.contains("valid")
                        )
                )
        );
    }

    private Dataset<Row> mkDataset() {
        return spark.createDataFrame(
                Arrays.asList(
                        new GenericRow(new Object[]{"0", "2000-01-01 00:00:00"}),
                        new GenericRow(new Object[]{"1", "2000-01-01 00:00:00"})
                ),
                StructType.fromDDL(
                        "id string, valid string"
                )
        );
    }

}
