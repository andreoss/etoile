/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.util;

import java.util.Map;
import java.util.function.Function;
import org.cactoos.iterable.Mapped;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapEnvelope;
import org.cactoos.map.MapOf;

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
            new MapOf<>(
                new Mapped<>(
                    e -> new MapEntry<>(
                        conv.apply(e.getKey()),
                        e.getValue()
                    ),
                    orig.entrySet()
                )
            )
        );
    }
}
