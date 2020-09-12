/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.nio.file.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

/**
 * Input from CSV files.
 *
 * @since 0.6.0
 */
public final class CsvInput extends TransformationEnvelope<Row> {
    /**
     * Secondary ctor.
     * @param session Spark session.
     * @param path Input path.
     */
    public CsvInput(final SparkSession session, final Path path) {
        this(session, path.toUri().toString());
    }

    /**
     * Secondary ctor.
     * @param session Spark session.
     * @param input Input file.
     */
    public CsvInput(final SparkSession session, final File input) {
        this(session, input.toPath());
    }

    /**
     * Ctor.
     * @param session Spark session.
     * @param path Path to files.
     */
    public CsvInput(final SparkSession session, final String path) {
        super(
            new Input(
                session,
                new MapOf<>(
                    new MapEntry<>("format", "csv"),
                    new MapEntry<>("header", "true"),
                    new MapEntry<>("path", path)
                )
            )
        );
    }
}
