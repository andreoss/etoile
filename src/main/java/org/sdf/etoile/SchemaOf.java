package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.types.StructType;

@RequiredArgsConstructor
public final class SchemaOf<X> implements Schema {
    private final Transformation<X> tran;
    @Override
    public StructType get() {
        return this.tran.get().schema();
    }
}
