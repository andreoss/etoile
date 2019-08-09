package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
final class PrefixArgs implements Map<String, String> {
    private final String prefix;
    private final Map<String, String> source;

    @Delegate
    private Map<String, String> get() {
        final Set<String> keys = this.source.keySet();
        final Map<String, String> result = new HashMap<>();
        for (final String key : keys) {
            final String fullPrefix = prefix + ".";
            if (key.startsWith(prefix))  {
               result.put(
                       key.replaceFirst(fullPrefix, ""),
                       this.source.get(key)
               );
           }
        }
        return Collections.unmodifiableMap(result);
    }
}
