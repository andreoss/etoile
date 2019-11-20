/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Casted by command-line parameters.
 *
 * @since 0.2.5
 */
final class ColumnsCastedByParameters extends Transformation.Envelope<Row> {
    /**
     * Ctor.
     * @param original Original tranformation.
     * @param key Key of paramaters.
     * @param params Parameters.
     */
    ColumnsCastedByParameters(final Transformation<Row> original,
        final String key, final Map<String, String> params) {
        super(
            () ->
                new ColumnsCastedToTypeMultiple(
                    original,
                    new Pairs(key, params)
                )
        );
    }
}
