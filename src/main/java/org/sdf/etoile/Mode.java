/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.util.EnumUtil;

/**
 * Output with certain behaviour on write.
 *
 * @param <X> Underlying data type.
 * @since 0.3.0
 */
@RequiredArgsConstructor
final class Mode<X> implements Output<X> {
    /**
     * Save mode.
     * Used on write
     */
    private final SaveMode type;

    /**
     * Original output.
     */
    private final Output<X> orig;

    /**
     * Secondary ctor.
     *
     * @param mode Mode as case-insensitive string
     * @param original Original output
     */
    Mode(final String mode, final Output<X> original) {
        this(EnumUtil.parseIgnoreCase(SaveMode.class, mode), original);
    }

    @Override
    public DataFrameWriter<X> get() {
        return this.orig.get()
            .mode(this.type);
    }
}
