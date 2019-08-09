package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.util.EnumUtil;

@RequiredArgsConstructor
final class Mode<X> implements Output<X> {
    private final SaveMode type;
    private final Output<X> orig;

    Mode(final String type, final Output<X> orig) {
        this(EnumUtil.parseIgnoreCase(SaveMode.class, type), orig);
    }

    @Override
    public DataFrameWriter<X> get() {
        return this.orig.get().mode(this.type);
    }
}
