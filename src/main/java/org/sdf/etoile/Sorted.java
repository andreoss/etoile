package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

@RequiredArgsConstructor
final class Sorted<Y> implements Transformation<Y> {
    private final Transformation<Y> df;
    private final Column[] columns;

    Sorted(final Transformation<Y> df, final String expr) {
        this(df, new Column[]{functions.expr(expr)});
    }

    @Override
    public Dataset<Y> get() {
        return this.df.get()
                .sort(this.columns);
    }
}
