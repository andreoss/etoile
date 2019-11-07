/*
 * Copyright(C) 2019
 */
package org.sdf.etoile.util;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.cactoos.map.MapEnvelope;

/**
 * Map keys.
 * @param <X> Resulting key type.
 * @param <K> Original key type.
 * @param <V> Value type.
 *
 * @since 0.2.5
 */
public final class MappedKeysMap<X, K, V> extends MapEnvelope<X, V> {
    /**
     * Ctor.
     * @param conv Convert function.
     * @param orig Original map.
     */
    public MappedKeysMap(final Function<K, X> conv, final Map<K, V> orig) {
        super(
            () -> {
                final Function<Map.Entry<K, V>, K> key = Map.Entry::getKey;
                return orig.entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(
                            key.andThen(conv),
                            Map.Entry::getValue
                        )
                    );
            }
        );
    }
}
