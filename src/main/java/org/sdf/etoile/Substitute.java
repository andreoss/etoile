/*
 * Copyright(C) 2019
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
     * Ctor.
     * @param replacements Dictionary
     */
    Substitute(final Map<Type, Map<Object, Object>> replacements) {
        super(new ValueReplacementByTypeMapFunction<>(replacements));
    }
}
