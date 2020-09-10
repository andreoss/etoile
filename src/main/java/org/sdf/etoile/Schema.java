/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.cactoos.list.ListOf;

/**
 * Schema.
 * @since 0.2.0
 */
@FunctionalInterface
public interface Schema extends Supplier<StructType> {

    /**
     * Convert to Map.
     * @return Column/type map
     */
    default Map<String, String> asMap() {
        return Arrays.stream(this.get().fields()).collect(
            Collectors.toMap(
                StructField::name,
                field -> field.dataType().catalogString()
            )
        );
    }

    /**
     * Extract field names.
     * @return Names
     */
    default List<String> fieldNames() {
        return new ListOf<>(
            this.get()
                .fieldNames()
        );
    }
}
