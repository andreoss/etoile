package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
final class PrefixArgs implements Map<String, String> {
    private final String prefix;
    private final Map<String, String> source;

    @Delegate
    private Map<String, String> get() {
        final String fullPrefix = prefix + ".";
        final Map<String, String> result = new HashMap<>();
        for (final Entry<String, String> entry : this.source.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (key.startsWith(prefix)) {
                result.put(
                        key.replaceFirst(fullPrefix, ""),
                        value
                );
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
