package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;

import java.util.Collections;
import java.util.Map;

@RequiredArgsConstructor
public final class HeaderCsvOutput<T> implements Output<Row> {
    private final Transformation<T> orig;
    private final Map<String, String> param;

    HeaderCsvOutput(
            final Transformation<T> source
    ) {
        this(source, Collections.emptyMap());
    }

    @Override
    public DataFrameWriter<Row> get() {
        if (param.containsKey("header")) {
            throw new IllegalArgumentException(
                    "header should not be set for `csv+header` format"
            );
        }
        final Transformation<Row> stringified = new Stringified<>(this.orig);
        return new ParameterizedOutput<>(
                new OrderedUnion<>(
                        new Header<>(stringified),
                        stringified
                ),
                param,
                "csv"
        ).get();
    }
}
