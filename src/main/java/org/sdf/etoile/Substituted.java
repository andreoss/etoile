package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;


final class ComposedMapFunction<A, B, C> extends MapFunctionEnvelop<A, C> {
    ComposedMapFunction(final MapFunction<A, B> first, final MapFunction<B, C> second) {
        super(a -> second.call(first.call(a)));
    }
}

@RequiredArgsConstructor
final class ValueReplacementByColumnMapFunction<V> implements MapFunction<Row, Row> {
    private final String col;
    private final Map<V, V> repl;

    @Override
    public Row call(final Row value) throws Exception {
        final int inx = value.fieldIndex(col);
        final List<Object> xs = new ArrayList<>(JavaConversions.asJavaCollection(value.toSeq()));
        final V val = value.getAs(inx);
        if (repl.containsKey(val)) {
            xs.set(inx, repl.get(val));
        }
        return new GenericRowWithSchema(xs.toArray(), value.schema());
    }
}

@RequiredArgsConstructor
final class ValueReplacementByTypeMapFunction<V> implements MapFunction<Row, Row> {
    private final Map<Type, Map<V, V>> repl;

    @Override
    public Row call(final Row value) throws Exception {
        MapFunction<Row, Row> composed = row -> row;
        final Collection<StructField> xs = JavaConversions.asJavaCollection(value.schema());
        for (final Map.Entry<Type, Map<V, V>> ent : repl.entrySet()) {
            for (final StructField x : xs) {
                final DataType dt = x.dataType();
                if (!dt.equals(ent.getKey()
                        .value())) {
                    break;
                }
                final Map<V, V> map = Objects.requireNonNull(this.repl.get(ent.getKey()));
                composed = new ComposedMapFunction<>(
                        composed,
                        new ValueReplacementByColumnMapFunction<>(
                                x.name(),
                                map
                        ));
            }
        }
        return composed.call(value);
    }
}


final class Substituted extends Transformation.Envelope<Row> {
    Substituted(final Transformation<Row> input, final Map<Type, Map<Object, Object>> dict) {
        super(() -> new MappedTransformation(
                input,
                new Substitute(dict)
        ));
        if (!(dict instanceof Serializable)) {
            throw new IllegalArgumentException(
                    String.format("%s is not Serializable",
                            dict.getClass()
                                    .getCanonicalName()
                    )
            );
        }
    }
}

