package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

    public FakeInput(final SparkSession session, final String ddl, final List<Object[]> rows) {
        this(
                session,
                StructType.fromDDL(ddl),
                rows.stream()
                        .map(row -> new GenericRowWithSchema(row, StructType.fromDDL(ddl)))
                        .collect(Collectors.toList())
        );
    }

    @Override
    public Dataset<Row> get() {
        return session.createDataFrame(
                rows,
                ddl
        );
    }

}
