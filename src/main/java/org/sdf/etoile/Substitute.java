package org.sdf.etoile;

import org.apache.spark.sql.Row;

import java.util.Map;

final class Substitute extends MapFunctionEnvelop<Row, Row> {
    Substitute(final Map<Type, Map<Object, Object>> replacements) {
        super(new ValueReplacementByTypeMapFunction<>(replacements));
    }
}
