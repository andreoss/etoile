/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Output with specified format.
 *
 * @param <T> Underlying data type.
 * @since 0.3.2
 */
final class FormatOutput<T> extends Output.Envelope<Row> {

    /**
     * Format option name.
     */
    public static final String FORMAT = "format";

    /**
     * Csv format name.
     */
    private static final String CSV = "csv";

    /**
     * Ctor.
     * @param input Original trainformation.
     * @param parameters Parameters.
     */
    FormatOutput(final Transformation<T> input,
        final Map<String, String> parameters) {
        super(
            () -> {
                final Map<String, String> copy = new HashMap<>(parameters);
                final Transformation<Row> res;
                if (
                    "csv+header".equals(
                        parameters.getOrDefault(
                            FormatOutput.FORMAT,
                            FormatOutput.CSV
                        )
                    )
                ) {
                    if (parameters.containsKey("header")) {
                        throw new IllegalArgumentException(
                            "header should not be set for `csv+header` format"
                        );
                    }
                    copy.put(FormatOutput.FORMAT, FormatOutput.CSV);
                    res = new StringifiedWithHeader<>(input);
                } else {
                    res = new Noop<>(input.get().toDF());
                }
                return new Mode<>(
                    parameters.getOrDefault(
                        "mode",
                        SaveMode.ErrorIfExists.name()
                    ),
                    new ParameterizedOutput<>(
                        res,
                        copy,
                        copy.getOrDefault(FormatOutput.FORMAT, FormatOutput.CSV)
                    )
                );
            }
        );
    }

}
