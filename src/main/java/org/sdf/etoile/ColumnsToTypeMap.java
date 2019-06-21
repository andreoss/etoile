package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
final class ColumnsToTypeMap implements List<Map<String, String>> {
    private final Map<String, List<String>> typeToColumns;
    private final List<Map<String, String>> typeToType;

    @Delegate
    private List<Map<String, String>> value() {
        final List<Map<String, String>> result = new ArrayList<>();
        for (final Map<String, String> type : typeToType) {
            for (final Map.Entry<String, String> entry : type.entrySet()) {
                final String from = entry.getKey();
                final String to = entry.getValue();
                if (typeToColumns.containsKey(from)) {
                    final List<String> xs = typeToColumns.get(from);
                    for (final String x : xs) {
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
