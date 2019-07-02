package org.sdf.etoile;

import org.apache.spark.sql.Row;

import java.util.Map;

final class StoredOutput<T> extends Output.Envelope<Row> {

    StoredOutput(
            final Transformation<T> input,
            final Map<String, String> parameters
    ) {
        super(() -> {
                    final String codec = parameters.getOrDefault(
                            "format", "csv"
                    );
                    if ("csv+header".equals(codec)) {
                        return new HeaderCsvOutput<>(
                                input,
                                parameters
                        );
                    } else {
                        return new ParameterizedOutput<>(
                                new Transformation.Noop<>(
                                        input.get()
                                                .toDF()
                                ),
                                parameters,
                                codec
                        );
                    }
                }
        );
    }

}
