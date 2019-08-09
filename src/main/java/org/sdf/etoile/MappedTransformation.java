package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@RequiredArgsConstructor
public final class MappedTransformation implements Transformation<Row> {
    private final Transformation<Row> orig;
    private final MapFunction<Row, Row> map;
    @Override
    public Dataset<Row> get() {
        final Dataset<Row> dataset = orig.get();
        return dataset.map(map, dataset.exprEnc());
    }
}
