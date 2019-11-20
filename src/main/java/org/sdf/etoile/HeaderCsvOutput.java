/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;

/**
 * CSV Output with header.
 * Required since original `csv` format produces empty files for empty dataframes.
 * @param <T> Underlying data type.
 * @todo Refactor in order to remove parameters.
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class HeaderCsvOutput<T> implements Output<Row> {
    /**
     * Original tranfomation.
     */
    private final Transformation<T> orig;

    /**
     * Parameters.
     */
    private final Map<String, String> param;

    /**
     * Ctor.
     * @param source Original transformation.
     */
    HeaderCsvOutput(final Transformation<T> source) {
        this(source, Collections.emptyMap());
    }

    @Override
    public DataFrameWriter<Row> get() {
        if (this.param.containsKey("header")) {
            throw new IllegalArgumentException(
                "header should not be set for `csv+header` format"
            );
        }
        final Transformation<Row> stringified = new Stringified<>(this.orig);
        return new ParameterizedOutput<>(
            new OrderedUnion<>(new Header<>(stringified), stringified),
            this.param,
            "csv"
        ).get();
    }
}
