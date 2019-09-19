package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

@RequiredArgsConstructor
final class Input implements Transformation<Row> {
    private static final String AVRO = "com.databricks.spark.avro";
    private final SparkSession spark;
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        final Dataset<Row> result;
        final String format = params.getOrDefault("format", AVRO);
        if (format.equals("avro+missing")) {
            final Dataset<Row> raw = this.spark.read()
                    .format(AVRO)
                    .options(params)
                    .load();
            result = new Demissingified(raw).get();
        } else {
            result = this.spark.read()
                    .format(format)
                    .options(params)
                    .load();
        }
        return result;
    }
}
