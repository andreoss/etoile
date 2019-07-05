package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public final class Header<Y> extends Transformation.Envelope<Row> {
    public Header(final Transformation<Row> df) {
        this(df.get());
    }

    public Header(final Dataset<Row> df) {
        super(() -> {
            final Schema schema = new SchemaOf<>(new Stringified<>(() -> df));
            return new Rows(
                    df.sparkSession(),
                    schema,
                    new GenericRow(schema.get()
                            .fieldNames())
            );
        });
    }
}
