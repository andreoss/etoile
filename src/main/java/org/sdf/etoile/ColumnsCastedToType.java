/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Casted to type.
 * @param <Y> Underlying data type.
 *
 * @since 0.2.5
 */
@RequiredArgsConstructor
final class ColumnsCastedToType<Y> implements Transformation<Row> {
    /**
     * Original tranfomation.
     */
    private final Transformation<Y> original;

    /**
     * Column/type map.
     */
    private final Map<String, String> casts;

    @Override
    public Dataset<Row> get() {
        Dataset<Row> memo = this.original.get().toDF();
        for (final Map.Entry<String, String> entry : this.casts.entrySet()) {
            final String name = entry.getKey();
            final String type = entry.getValue();
            final Column cast = memo.col(name).cast(type);
            memo = memo.withColumn(name, cast);
        }
        return memo;
    }
}
