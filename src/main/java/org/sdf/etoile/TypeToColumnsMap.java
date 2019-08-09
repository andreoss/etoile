package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.cactoos.collection.Joined;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEnvelope;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TypeToColumnsMap extends MapEnvelope<DataType, List<String>> {

    private TypeToColumnsMap(final Dataset<Row> df) {
        this(df.schema());
    }

    TypeToColumnsMap(final Transformation<Row> transformation) {
        this(transformation.get());
    }

    private TypeToColumnsMap(final StructType schema) {
        super(() -> {
            final Function<StructField, DataType> type = StructField::dataType;
            final Function<StructField, String> name = StructField::name;
            return Stream.of(schema
                    .fields())
                    .collect(
                            Collectors.toMap(
                                    type,
                                    name.andThen(Collections::singletonList),
                                    (a, b) -> new ListOf<>(new Joined<>(a, b))
                            )
                    );
        });
    }


}
