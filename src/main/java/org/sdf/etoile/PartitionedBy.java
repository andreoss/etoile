/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;

/**
 * Partitioned output.
 * @param <X> Underlying type.
 *
 * @since 0.6.0
 */
@RequiredArgsConstructor
public final class PartitionedBy<X> implements Output<X> {
    /**
     * Partition columns.
     */
    private final String[] cols;

    /**
     * Original output.
     */
    private final Output<X> output;

    /**
     * Secondary ctor.
     * @param column Column.
     * @param output Original output.
     */
    public PartitionedBy(final String column, final Output<X> output) {
        this(new String[]{column}, output);
    }

    @Override
    public DataFrameWriter<X> get() {
        return this.output.get().partitionBy(this.cols);
    }
}
