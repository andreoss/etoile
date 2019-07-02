package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;

import java.util.Map;

@RequiredArgsConstructor
public final class ParameterizedOutput<Y> implements Output<Y> {
    private final Transformation<Y> tran;
    private final Map<String, String> param;
    private final String format;

    @Override
    public DataFrameWriter<Y> get() {
        return this.tran.get()
                .write()
                .format(format)
                .options(param);
    }
}
