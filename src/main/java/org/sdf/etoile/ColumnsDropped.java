package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
public final class ColumnsDropped<Y> implements Transformation<Row> {
    private final Transformation<Y> orig;
    private final List<String> columns;

    ColumnsDropped(final Transformation<Y> orig, final String column) {
        this(orig, Collections.singletonList(column));
    }

    @Override
    public Dataset<Row> get() {
        Dataset<Row> copy = this.orig.get()
                .toDF();
        for (final String column : columns) {
            copy = copy.drop(copy.col(column));
        }
        return copy;
    }
}
