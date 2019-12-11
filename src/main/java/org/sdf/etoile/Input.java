/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import com.databricks.spark.avro.AvroOutputWriter;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Input from parameters.
 *
 * @since 0.1.0
 */
@RequiredArgsConstructor
final class Input implements Transformation<Row> {
    /**
     * Parameter for table instead of "path".
     */
    private static final String TABLE_PARAM = "table";

    /**
     * Default format.
     */
    private static final String AVRO = AvroOutputWriter.class
        .getPackage().getName();

    /**
     * Spark session.
     */
    private final SparkSession spark;

    /**
     * Input parameters.
     */
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        final Dataset<Row> result;
        final String format = this.params.getOrDefault("format", Input.AVRO);
        if (format.endsWith("+missing")) {
            final Dataset<Row> raw = this.spark.read()
                    .format(Input.AVRO)
                    .options(this.params)
                    .load();
            result = (new Demissingified(raw)).get();
        } else if (this.params.containsKey(Input.TABLE_PARAM)) {
            result = this.spark.table(this.params.get(Input.TABLE_PARAM));
        } else {
            result = this.spark.read()
                .format(format)
                .options(this.params)
                .load();
        }
        return result;
    }
}
