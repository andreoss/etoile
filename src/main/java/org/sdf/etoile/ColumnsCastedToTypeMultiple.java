package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
final class ColumnsCastedToTypeMultiple implements Transformation<Row> {
    private final Transformation<Row> first;
    private final Collection<Map<String, String>> casts;

    @Override
    public Dataset<Row> get() {
        Transformation<Row> copy = first;
        for (final Map<String, String> cast : casts) {
            copy = new ColumnsCastedToType<>(copy, cast);
        }
        return copy.get();
    }
}
