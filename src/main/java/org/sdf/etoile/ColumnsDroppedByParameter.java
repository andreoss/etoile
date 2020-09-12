/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Tranformation with removed columns.
 *
 * @param <X> Underlying data type.
 *
 * @since 0.3.0
 */
final class ColumnsDroppedByParameter<X> extends TransformationEnvelope<Row> {
    /**
     * Key for parameters.
     */
    private static final String KEY = "drop";

    /**
     * Ctor.
     * @param original Original transformation.
     * @param params Parameters.
     */
    ColumnsDroppedByParameter(final Transformation<X> original, final Map<String, String> params) {
        super(
            new ConditionalTransformation<>(
                () -> params.containsKey(ColumnsDroppedByParameter.KEY),
                () -> new WithoutColumns<>(
                    original,
                    Arrays.asList(
                        params.get(
                            ColumnsDroppedByParameter.KEY
                        ).split(",")
                    )
                ).get(),
                new Noop<>(original.get().toDF())
            )
        );
    }
}

