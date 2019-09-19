package org.sdf.etoile;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

final class ColumnsDroppedTest {
    private SparkSession session;

    @BeforeEach
    void setUp() {
        session = SparkSession.builder()
                .master("local[1]")
                .getOrCreate();
    }

    @Test
    void dropsColumnsFromDataset() {
        final Transformation<Row> x = new ColumnsDropped<>(
                new FakeInput(session, "id int, removed string"),
                "removed"
        );
        MatcherAssert.assertThat(
                "column was dropped",
                x.get()
                        .schema()
                        .fieldNames(),
                Matchers.arrayContaining(
                        "id"
                )
        );
    }

    @Test
    void failsOnUnresolvebleColumn() {
        final Transformation<Row> x = new ColumnsDropped<>(
                new FakeInput(session, "id int, removed string"),
                "notfound"
        );
        Assertions.assertThrows(AnalysisException.class, x::get);
    }

    @Test
    void dropsSeveralColumns() {
        final Transformation<Row> x = new ColumnsDropped<>(
                new FakeInput(session, "id int, removed string, alsoremoved timestamp"),
                Arrays.asList("removed", "alsoremoved")
        );
        MatcherAssert.assertThat(
                "column was dropped",
                x.get()
                        .schema()
                        .fieldNames(),
                Matchers.arrayContaining(
                        "id"
                )
        );
    }

}
