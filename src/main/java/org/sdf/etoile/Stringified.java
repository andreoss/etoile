package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType$;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

public final class Stringified<Y> extends Transformation.Envelope<Row> {
    public Stringified(final Transformation<Y> tran) {
        super(
                new ColumnsCastedToType<Y>(
                        tran,
                        new MapOf<>(
                                f -> new MapEntry<>(
                                        f,
                                        StringType$.MODULE$.catalogString()
                                ),
                                new SchemaOf<>(tran).fieldNames()
                        )
                )
        );
    }
}
