/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Output with specified format.
 *
 * @param <T> Underlying data type.
 * @since 0.3.2
 */
final class FormatOutput<T> extends Output.Envelope<Row> {

    /**
     * Ctor.
     * @param input Original trainformation.
     * @param parameters Parameters.
     */
    FormatOutput(final Transformation<T> input,
        final Map<String, String> parameters) {
        super(
            () -> {
                final Output<Row> result;
                final String codec = parameters.getOrDefault("format", "csv");
                if ("csv+header".equals(codec)) {
                    result = new HeaderCsvOutput<>(input, parameters);
                } else {
                    result = new ParameterizedOutput<>(
                        new Transformation.Noop<>(input.get().toDF()),
                        parameters,
                        codec
                    );
                }
                return result;
            }
        );
    }
}
