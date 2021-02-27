/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.cactoos.iterable.IterableOf;
import org.cactoos.iterator.Mapped;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;

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
    public boolean call(final Row value) throws Exception {
        final Map<String, String> pvs = new ExtractPartitions().call(
            value.<String>getAs(this.column)
        );
        final Map<String, String> cvs =
            new MapOf<>(
                new IterableOf<>(
                    new Mapped<>(
                        k -> new MapEntry<>(
                            k,
                            Objects.toString(value.getAs(k), "NULL")
                        ),
                        pvs.keySet().iterator()
                    )
                )

            );
        return pvs.equals(cvs);
    }
}
