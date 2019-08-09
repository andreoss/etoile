package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.util.EnumUtil;

@RequiredArgsConstructor
final class Mode<X> implements Output<X> {
    private final String type;
    private final Output<X> orig;

    @Override
    public DataFrameWriter<X> get() {
        return this.orig.get()
                .mode(EnumUtil.parseIgnoreCase(SaveMode.class, this.type));
    }

}
