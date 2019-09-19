package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

import java.util.Map;

final class Demissingified extends Transformation.Envelope<Row> {
    Demissingified(final Dataset<Row> original) {
        super(() -> {
            final Schema schema = new SchemaOf<>(() -> original);
            final Map<String, String> map = new MapOf<>(
                    name -> new MapEntry<>(
                            name, String.format("missing(%s)", name)
                    ),
                    schema.fieldNames()
            );
            return new ExpressionTransformed(
                    new Stringified<>(() -> original), new ListOf<>(map)
            );
        });
    }
}
