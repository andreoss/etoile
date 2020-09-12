/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Casted by command-line parameters.
 *
 * @since 0.2.5
 */
final class ColumnsCastedByParameters extends TransformationEnvelope<Row> {
    /**
     * Ctor.
     * @param original Original transformation.
     * @param key Key of parameters.
     * @param params Parameters.
     */
    ColumnsCastedByParameters(final Transformation<Row> original,
        final String key, final Map<String, String> params) {
        super(
            new ColumnsCastedToTypeMultiple(
                original,
                new Pairs(key, params)
            )
        );
    }
}
