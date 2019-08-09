package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
final class ValueReplacementByColumnMapFunction<V>
        implements MapFunction<Row, Row> {
    private final String col;
    private final Map<V, V> repl;

    @SuppressWarnings("RedundantThrows")
    @Override
    public Row call(final Row value) throws Exception {
        final int inx = value.fieldIndex(col);
        final List<Object> xs = new ArrayList<>(
                JavaConversions.asJavaCollection(value.toSeq())
        );
        final V val = value.getAs(inx);
        if (repl.containsKey(val)) {
            xs.set(inx, repl.get(val));
        }
        return new GenericRowWithSchema(xs.toArray(), value.schema());
    }
}
