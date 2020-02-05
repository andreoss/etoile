/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 * Filter rows with correct partition scheme.
 *
 * @since 0.6.0
 */
@RequiredArgsConstructor
final class PartitionValuesMatch implements FilterFunction<Row> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 6466635419179618159L;

    /**
     * Input file column.
     */
    private final String column;

    @Override
    public boolean call(final Row value) {
        final String file = value.getAs(this.column);
        final Map<String, String> pvs = new ExtractPartitions()
            .apply(file);
        final Map<String, String> cvs = pvs.keySet().stream()
            .map(
                k -> Collections.singletonMap(
                    k,
                    Objects.toString(value.getAs(k), "NULL")
                )
            )
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue
                )
            );
        return pvs.equals(cvs);
    }
}
