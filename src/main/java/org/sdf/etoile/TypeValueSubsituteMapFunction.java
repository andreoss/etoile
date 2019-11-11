/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

/**
 * Map function for replacement by dictionary.
 *
 * @param <V> Type of values to replace.
 *
 * @since 0.3.0
 */
@RequiredArgsConstructor
final class TypeValueSubsituteMapFunction<V> implements MapFunction<Row, Row> {
    /**
     * Dictionary.
     */
    private final Map<Type, Map<V, V>> repl;

    @Override
    public Row call(final Row value) throws Exception {
        MapFunction<Row, Row> composed = row -> row;
        final Collection<StructField> fields = JavaConversions.asJavaCollection(
            value.schema()
        );
        for (final Map.Entry<Type, Map<V, V>> ent : this.repl.entrySet()) {
            for (final StructField field : fields) {
                final DataType type = field.dataType();
                if (type.sameType(ent.getKey().asSpark())) {
                    final Map<V, V> map =
                        Objects.requireNonNull(this.repl.get(ent.getKey()));
                    composed = new Composed<>(
                        composed,
                        new ValueReplacementByColumnMapFunction<>(
                            field.name(), map
                        )
                    );
                }
            }
        }
        return composed.call(value);
    }
}
