package org.sdf.etoile;

import org.cactoos.collection.CollectionEnvelope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class ColumnsToTypeMap extends CollectionEnvelope<Map<String, String>> {

    ColumnsToTypeMap(
            final Map<String, List<String>> typeToColumns,
            final Collection<Map<String, String>> typeToType
    ) {
        super(() -> mkMap(typeToColumns, typeToType));
    }

    private static <T, C> Collection<Map<C, T>> mkMap(
            final Map<T, List<C>> typeToColumns,
            final Collection<Map<T, T>> typeToType
    ) {
        final Collection<Map<C, T>> result = new ArrayList<>();
        for (final Map<T, T> type : typeToType) {
            for (final Map.Entry<T, T> entry : type.entrySet()) {
                final T from = entry.getKey();
                final T to = entry.getValue();
                if (typeToColumns.containsKey(from)) {
                    final List<C> xs = typeToColumns.get(from);
                    for (final C x : xs) {
                        result.add(
                                Collections.singletonMap(x, to)
                        );
                    }
                }
            }
        }
        return result;
    }
}