/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Input processed according to command-line parameters.
 *
 * @since 0.4.0
 */
final class ProcessedInput extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     * @param input Raw input.
     * @param parameters Command-line parameters.
     */
    ProcessedInput(final Transformation<Row> input,
        final Map<String, String> parameters) {
        super(
            new SortedByParameter<>(
                new ExpressionTransformed(
                    new FullyCastedByParameters(
                        input,
                        parameters
                    ),
                    parameters
                ),
                parameters
            )
        );
    }
}

