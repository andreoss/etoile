package org.sdf.etoile;

import org.apache.spark.sql.Row;

import java.util.Map;

final class ColumnsCastedByParameters extends Transformation.Envelope<Row> {
    ColumnsCastedByParameters(
            final Transformation<Row> df,
            final String key,
            final Map<String, String> params
    ) {
        super(() ->
                new ColumnsCastedToTypeMultiple(
                        df,
                        new CastParameters(key, params)
                )
        );
    }
}
