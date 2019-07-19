package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

@RequiredArgsConstructor
final class ValueReplacementByTypeMapFunction<V>
        implements MapFunction<Row, Row> {
    private final Map<Type, Map<V, V>> repl;

    @Override
    public Row call(final Row value) throws Exception {
        MapFunction<Row, Row> composed = row -> row;
        final Collection<StructField> xs =
                JavaConversions.asJavaCollection(value.schema());
        for (final Map.Entry<Type, Map<V, V>> ent : repl.entrySet()) {
            for (final StructField x : xs) {
                final DataType dt = x.dataType();
                if (dt.sameType(ent.getKey()
                        .value())) {
                    final Map<V, V> map =
                            Objects.requireNonNull(
                                    this.repl.get(ent.getKey())
                            );
                    composed = new ComposedMapFunction<>(
                            composed,
                            new ValueReplacementByColumnMapFunction<>(
                                    x.name(),
                                    map
                            ));
                }
            }
        }
        return composed.call(value);
    }
}
