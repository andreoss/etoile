package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
final class FakeInput implements Transformation<Row> {
    private final SparkSession session;
    private final StructType ddl;
    private final List<Row> rows;

    private FakeInput(final SparkSession session, final StructType ddl) {
        this(session, ddl, Collections.emptyList());
    }

    FakeInput(final SparkSession session, final String ddl) {
        this(session, StructType.fromDDL(ddl));
    }

    @Override
    public Dataset<Row> get() {
        return session.createDataFrame(
                rows,
                ddl
        );
    }
}

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
