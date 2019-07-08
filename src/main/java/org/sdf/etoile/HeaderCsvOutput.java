package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

@RequiredArgsConstructor
public final class HeaderCsvOutput<T> implements Terminal {
    private final Transformation<T> orig;
    private final Path result;
    private final Map<String, String> param;

    HeaderCsvOutput(
            final Transformation<T> source,
            final Path output
    ) {
        this(source, output, Collections.emptyMap());
    }

    @Override
    public Path result() {
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
                "csv",
                result
        ).result();
    }
}
