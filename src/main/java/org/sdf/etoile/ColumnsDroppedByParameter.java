/*
 * Copyright(C) 2019. See COPYING for more.
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
final class ColumnsDroppedByParameter<X> extends Transformation.Envelope<Row> {
    /**
     * Key for parameters.
     */
    private static final String KEY = "drop";

    /**
     * Ctor.
     * @param original Original tranformation.
     * @param params Parameters.
     */
    ColumnsDroppedByParameter(final Transformation<X> original, final Map<String, String> params) {
        super(
            () -> {
                final Transformation<Row> result;
                if (params.containsKey(ColumnsDroppedByParameter.KEY)) {
                    result = new WithoutColumns<>(
                        original,
                        Arrays.asList(params.get(ColumnsDroppedByParameter.KEY).split(","))
                    );
                } else {
                    result = new Transformation.Noop<>(
                        original.get().toDF()
                    );
                }
                return result;
            }
        );
    }
}

