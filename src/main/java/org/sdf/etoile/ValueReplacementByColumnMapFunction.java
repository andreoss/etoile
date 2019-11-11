/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.JavaConversions;

/**
 * Map function for column value replacement.
 * @param <V> Value type.
 * @since 0.3.0
 */
@RequiredArgsConstructor
final class ValueReplacementByColumnMapFunction<V> implements MapFunction<Row, Row> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -303840972102172626L;

    /**
     * Column name.
     */
    private final String col;

    /**
     * Dictionary.
     */
    private final Map<V, V> dict;

    @Override
    public Row call(final Row value) {
        final int inx = value.fieldIndex(this.col);
        final List<Object> unpacked = new ArrayList<>(
            JavaConversions.asJavaCollection(value.toSeq())
        );
        final V val = value.getAs(inx);
        if (this.dict.containsKey(val)) {
            unpacked.set(inx, this.dict.get(val));
        }
        return new GenericRowWithSchema(unpacked.toArray(), value.schema());
    }
}
