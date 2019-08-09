package org.sdf.etoile;

import org.cactoos.map.MapEnvelope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class PrefixArgs extends MapEnvelope<String, String> {
    PrefixArgs(final String prefix, final Map<String, String> source) {
        super(() -> {
            final String fullPrefix = prefix + ".";
            final Map<String, String> result = new HashMap<>();
            for (final Entry<String, String> entry : source.entrySet()) {
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
        });
    }

}
