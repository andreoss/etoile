package org.sdf.etoile;

import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Map;

final class ColumnsDroppedByParameter<X> extends Transformation.Envelope<Row> {
    ColumnsDroppedByParameter(final Transformation<X> df, final Map<String, String> params) {
        super(() -> {
                    final Transformation<Row> result;
                    if (params.containsKey("drop")) {
                        result = new ColumnsDropped<>(
                                df,
                                Arrays.asList(params.get("drop")
                                        .split(","))
                        );
                    } else {
                        result = new Noop<>(
                                df.get()
                                        .toDF()
                        );
                    }
                    return result;
                }
        );
    }
}
