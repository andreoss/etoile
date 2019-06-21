package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
final class TypeToColumnsMap implements Map<DataType, List<String>> {
    private final StructType schema;

    private TypeToColumnsMap(final Dataset<Row> df) {
        this(df.schema());
    }

    TypeToColumnsMap(final Transformation<Row> transformation) {
        this(transformation.get());
    }


    @Delegate
    private Map<DataType, List<String>> value() {
        final Function<StructField, DataType> type = StructField::dataType;
        final Function<StructField, String> name = StructField::name;
        return Stream.of(this.schema
                .fields())
                .collect(
                        Collectors.toMap(
                                type,
                                name.andThen(Collections::singletonList),
                                (a, b) -> new JoinedList<>(a, b)
                        )
                );
    }
}
