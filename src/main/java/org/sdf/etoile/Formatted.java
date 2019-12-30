/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Input with certain format, options and path.
 *
 * @since 0.5.0
 */
@RequiredArgsConstructor
final class Formatted implements Transformation<Row> {
    /**
     * Spark session.
     */
    private final SparkSession spark;

    /**
     * Format of data.
     */
    private final String format;

    /**
     * Options.
     */
    private final Map<String, String> opts;

    /**
     * Paths to load.
     */
    private final String[] paths;

    /**
     * Secondary ctor.
     * @param spark Spark session.
     * @param format Format.
     * @param opts Options.
     */
    Formatted(final SparkSession spark, final String format,
        final Map<String, String> opts) {
        this(spark, format, opts, new String[]{});
    }

    @Override
    public Dataset<Row> get() {
        return this.spark
            .read()
            .format(this.format)
            .options(this.opts)
            .load(this.paths);
    }
}
