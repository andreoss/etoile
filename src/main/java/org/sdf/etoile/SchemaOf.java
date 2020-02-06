/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.types.StructType;

/**
 * Schema of {@link Transformation}.
 * @param <X> Tranformations underlying type.
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class SchemaOf<X> implements Schema {
    /**
     * Tranformation to extract schema from.
     */
    private final Transformation<X> tran;

    @Override
    public StructType get() {
        return this.tran.get().schema();
    }
}
