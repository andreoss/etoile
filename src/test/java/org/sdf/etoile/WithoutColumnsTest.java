/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Arrays;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link WithoutColumns}.
 *
 * @since 0.2.0
 */
final class WithoutColumnsTest extends SparkTestTemplate {
    @Test
    void dropsColumnsFromDataset() {
        final Transformation<Row> trns = new WithoutColumns<>(
            new FakeInput(this.session, "id int, val string"),
            "val"
        );
        MatcherAssert.assertThat(
            "column was dropped",
            new SchemaOf(trns).fieldNames(),
            Matchers.contains("id")
        );
    }

    @Test
    void failsOnUnresolvebleColumn() {
        final Transformation<Row> trns = new WithoutColumns<>(
            new FakeInput(this.session, "id int, name string"),
            "notfound"
        );
        Assertions.assertThrows(AnalysisException.class, trns::get);
    }

    @Test
    void dropsSeveralColumns() {
        final Transformation<Row> trns = new WithoutColumns<>(
            new FakeInput(this.session, "id int, removed string, alsoremoved timestamp"),
            Arrays.asList("removed", "alsoremoved")
        );
        MatcherAssert.assertThat(
            "two columns were dropped",
            new SchemaOf(trns).fieldNames(),
            Matchers.contains("id")
        );
    }
}
