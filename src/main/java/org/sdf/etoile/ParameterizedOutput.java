/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;

/**
 * Parametarized {@link Output}.
 * @param <Y> Underlying data type.
 * @since 0.1.2
 */
@RequiredArgsConstructor
public final class ParameterizedOutput<Y> implements Output<Y> {
    /**
     * Source tranformation.
     */
    private final Transformation<Y> tran;

    /**
     * Options of {@link DataFrameWriter}.
     */
    private final Map<String, String> param;

    /**
     * Format.
     */
    private final String format;

    @Override
    public DataFrameWriter<Y> get() {
        return this.tran.get().write().format(this.format).options(this.param);
    }
}
