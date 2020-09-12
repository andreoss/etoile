/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.cactoos.collection.CollectionEnvelope;
import org.cactoos.list.ListOf;

/**
 * Each column to type.
 *
 * @since 0.2.5
 */
final class ColumnsToTypeMap extends CollectionEnvelope<Map<String, String>> {

    /**
     * Ctor.
     * @param typcol Type co column mapping.
     * @param typtyp Type to type mapping.
     */
    ColumnsToTypeMap(final Map<String, List<String>> typcol,
        final Collection<Map<String, String>> typtyp) {
        super(
            new ListOf<Map<String, String>>(
                () -> {
                    final Collection<Map<String, String>> result =
                        new ArrayList<>(typtyp.size());
                    for (final Map<String, String> type : typtyp) {
                        ColumnsToTypeMap.buildColumnToTypeMapping(
                            typcol,
                            result,
                            type
                        );
                    }
                    return result.iterator();
                }
            )
        );
    }

    /**
     * Build mapping for one type.
     * @param typcol Type to column.
     * @param result Aggregate to result.
     * @param type Type mapping.
     */
    private static void buildColumnToTypeMapping(final Map<String, List<String>> typcol,
        final Collection<Map<String, String>> result, final Map<String, String> type) {
        for (final Map.Entry<String, String> entry : type.entrySet()) {
            final String src = entry.getKey();
            final String dst = entry.getValue();
            if (typcol.containsKey(src)) {
                final List<String> cols = typcol.get(src);
                for (final String col : cols) {
                    result.add(Collections.singletonMap(col, dst));
                }
            }
        }
    }
}
