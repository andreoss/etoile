package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@RequiredArgsConstructor
final class Input implements Transformation<Row> {
    private static final String AVRO = "com.databricks.spark.avro";
    private final SparkSession spark;
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        final Dataset<Row> result;
        final String format = params.getOrDefault("format", AVRO);
        if (format.endsWith("+missing")) {
            final Dataset<Row> raw = this.spark.read()
                .format(removeModifier(format))
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

    private String removeModifier(final String format) {
        return format.replaceFirst("[+]missing$", "");
    }
}
