/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Input from parameters.
 *
 * @since 0.5.0
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
    private static final String AVRO = "avro";

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
            result = new Demissingified(
                new Formatted(
                    this.spark, Input.removeModifier(format), this.params
                )
            ).get();
        } else if (this.params.containsKey(Input.TABLE_PARAM)) {
            result = this.spark.table(this.params.get(Input.TABLE_PARAM));
        } else {
            result = new Formatted(
                this.spark, format, this.params
            ).get();
        }
        return result;
    }

    /**
     * Remove modifier.
     *
     * @param format Original
     * @return Without modifier
     */
    private static String removeModifier(final String format) {
        String fmt = Pattern.compile("[+]missing$").matcher(format).replaceFirst("");
        if ("avro".equalsIgnoreCase(fmt)) {
            fmt = Input.AVRO;
        }
        return fmt;
    }
}
