package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

@RequiredArgsConstructor
final class Input implements Transformation<Row> {
    private static final String CODEC = "com.databricks.spark.avro";
    private final SparkSession spark;
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        return this.spark.read()
                .format(params.getOrDefault("format", CODEC))
                .options(params)
                .load();
    }
}
