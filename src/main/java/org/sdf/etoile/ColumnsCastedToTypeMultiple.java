/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collection;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Casted with multiple casts.
 *
 * @since 0.2.5
 */
@RequiredArgsConstructor
final class ColumnsCastedToTypeMultiple implements Transformation<Row> {
    /**
     * Original tranformation.
     */
    private final Transformation<Row> original;

    /**
     * Collection of casts.
     */
    private final Collection<Map<String, String>> casts;

    @Override
    public Dataset<Row> get() {
        Transformation<Row> copy = this.original;
        for (final Map<String, String> cast : this.casts) {
            copy = new ColumnsCastedToType<>(copy, cast);
        }
        return copy.get();
    }
}
