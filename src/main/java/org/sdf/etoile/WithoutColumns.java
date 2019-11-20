/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;

/**
 * A transformation with columns dropped.
 *
 * @param <Y> Underlying type.
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class WithoutColumns<Y> implements Transformation<Row> {
    /**
     * An original tranformation.
     */
    private final Transformation<Y> orig;

    /**
     * Columns to drop.
     */
    private final List<String> columns;

    /**
     * Ctor.
     *
     * @param input An transformation.
     * @param cols Columns to drop.
     */
    WithoutColumns(final Transformation<Y> input, final String... cols) {
        this(input, new ListOf<>(cols));
    }

    @Override
    public Dataset<Row> get() {
        Dataset<Row> memo = this.orig.get().toDF();
        for (final String column : this.columns) {
            memo = memo.drop(memo.col(column));
        }
        return memo;
    }
}
