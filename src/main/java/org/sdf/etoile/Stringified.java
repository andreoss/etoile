/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnaryMathExpression;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StringType$;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

import java.util.Map;
import java.util.Objects;

/**
 * A tranformation which each column converted to `string`.
 * {@see org.apache.spark.sql.types.StringType}
 *
 * @param <Y> Underlying data type.
 * @since 0.3.2
 */
public final class Stringified<Y> extends TransformationEnvelope<Row> {

    /**
     * Ctor.
     *
     * @param original Dataset.
     */
    public Stringified(final Dataset<Y> original) {
        this(new Noop<>(original));
    }

    /**
     * Ctor.
     *
     * @param original Original tranfomation.
     */
    public Stringified(final Transformation<Y> original) {
        super(() -> {
                    final Schema schema = new SchemaOf<>(original);
                    final Map<String, String> kv = schema.asMap();
                    Dataset<Row> memo = original.get().toDF();
                    for (final Map.Entry<String, String> entry : kv.entrySet()) {
                        final String column = entry.getKey();
                        final String type = entry.getValue();
                        if (type.equalsIgnoreCase("binary")) {
                            memo = memo.withColumn(column, functions.base64(memo.col(column)));
                            // ???
                        } else {
                            memo = memo.withColumn(column, functions.col(column).cast("string"));
                        }
                    }
                    return memo;
                }
        );
    }
}

