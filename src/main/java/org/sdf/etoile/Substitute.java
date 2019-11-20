/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * A map function with substitutes values according to dictionary.
 * @since 0.3.0
 */
final class Substitute extends MapFunctionEnvelop<Row, Row> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -303840972102172626L;

    /**
     * Ctor.
     * @param replacements Dictionary
     */
    Substitute(final Map<Type, Map<Object, Object>> replacements) {
        super(new TypeValueSubsituteMapFunction<>(replacements));
    }
}
