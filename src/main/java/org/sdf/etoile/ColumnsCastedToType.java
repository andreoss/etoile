package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@RequiredArgsConstructor
final class ColumnsCastedToType<Y> implements Transformation<Row> {
    private final Transformation<Y> df;
    private final Map<String, String> columnToType;

    @Override
    public Dataset<Row> get() {
        Dataset<Row> copy = this.df.get()
                .toDF();
        for (final Map.Entry<String, String> entry : columnToType.entrySet()) {
            final String name = entry.getKey();
            final String type = entry.getValue();
            final Column cast = copy.col(name)
                    .cast(type);
            copy = copy.withColumn(name, cast);
        }
        return copy;
    }
}
