package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cactoos.list.ListOf;

import java.util.List;

@RequiredArgsConstructor
public final class Rows implements Transformation<Row> {
    private final SparkSession spark;
    private final Schema ddl;
    private final List<Row> elements;

    public Rows(
            final SparkSession session,
            final Schema schema,
            final Row... rows
    ) {
        this(session, schema, new ListOf<>(rows));
    }

    @Override
    public Dataset<Row> get() {
        return this.spark
                .createDataFrame(elements, this.ddl.get());
    }
}
