/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.cactoos.map.MapEnvelope;

/**
 * Arguments with prefix removed.
 *
 * @since 0.1.0
 */
final class PrefixArgs extends MapEnvelope<String, String> {
    /**
     * Ctor.
     * @param prefix Prefix to remove.
     * @param source Source map.
     */
    PrefixArgs(final String prefix, final Map<String, String> source) {
        super(
            () -> {
                final Map<String, String> result = new HashMap<>();
                for (final Map.Entry<String, String> ent : source.entrySet()) {
                    final String key = ent.getKey();
                    final String value = ent.getValue();
                    if (key.startsWith(prefix)) {
                        result.put(
                            key.replaceFirst(String.format("%s.", prefix), ""),
                            value
                        );
                    }
                }
                return Collections.unmodifiableMap(result);
            }
        );
    }

}
