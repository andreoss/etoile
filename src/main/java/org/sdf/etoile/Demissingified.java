package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public final class Demissingified implements Transformation<Row> {
    private final Dataset<Row> original;

    @Override
    public Dataset<Row> get() {
        final Schema schema = new SchemaOf<>(() -> original);
        final Map<String, String> map = new MapOf<>(
                name -> new MapEntry<>(name, String.format("missing(%s)", name)),
                schema.fieldNames()
        );
        final Collection<Map<String, String>> expressions = new ListOf<>(map);
        return new ExpressionTransformed(
                new Stringified<>(() -> original), expressions
        ).get();
    }
}
