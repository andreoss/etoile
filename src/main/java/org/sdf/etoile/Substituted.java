package org.sdf.etoile;

import org.apache.spark.sql.Row;

import java.util.Map;

final class Substituted extends Transformation.Envelope<Row> {
    Substituted(
            final Transformation<Row> input,
            final Map<Type, Map<Object, Object>> dict
    ) {
        super(() -> new MappedTransformation(
                input,
                new Substitute(dict)
        ));
        Verify.isSerializable(dict);
    }
}

