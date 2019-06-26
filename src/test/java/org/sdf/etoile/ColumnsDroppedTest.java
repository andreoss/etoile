package org.sdf.etoile;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public final class ColumnsDroppedTest {
    private SparkSession session;

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[1]")
                .getOrCreate();
    }

    @Test
    public void dropsColumnsFromDataset() {
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

    @Test(expected = AnalysisException.class)
    public void failsOnUnresolvebleColumn() {
        final Transformation<Row> x = new ColumnsDropped<>(
                new FakeInput(session, "id int, removed string"),
                "notfound"
        );
        x.get();
    }

    @Test
    public void dropsSeveralColumns() {
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
