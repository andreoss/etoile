package org.sdf.etoile;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.cactoos.list.ListOf;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface Schema extends Supplier<StructType> {

    default Map<String, String> asMap() {
        return Arrays.stream(this.get()
                .fields())
                .collect(
                        Collectors.toMap(
                                StructField::name,
                                field -> field.dataType()
                                        .catalogString()
                        )
                );
    }

    default List<String> fieldNames() {
        return new ListOf<>(
                this.get()
                        .fieldNames()
        );
    }
}
