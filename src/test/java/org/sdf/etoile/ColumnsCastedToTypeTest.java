/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ColumnsCastedToType}.
 *
 * @since 0.2.5
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class ColumnsCastedToTypeTest extends SparkTestTemplate {
    @Test
    void castsColumns() {
        MatcherAssert.assertThat(
            "casts column",
            new TypeToColumnsMap(
                new ColumnsCastedToType<>(
                    this::mkDataset,
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
    void castsSeveralColumns() {
        final Map<String, String> casts = new HashMap<>();
        casts.put("id", "int");
        casts.put("valid", "timestamp");
        MatcherAssert.assertThat(
            "casts column",
            new TypeToColumnsMap(
                new ColumnsCastedToType<>(
                    this::mkDataset,
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
        return SparkTestTemplate.session.createDataFrame(
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
