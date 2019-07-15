package org.sdf.etoile.util;

import org.cactoos.map.MapEnvelope;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class MappedKeysMap<X, K, V> extends MapEnvelope<X, V> {
    public MappedKeysMap(final Function<K, X> asString, final Map<K, V> orig) {
        super(() -> {
                    final Function<Entry<K, V>, K> key = Entry::getKey;
                    return orig.entrySet()
                            .stream()
                            .collect(
                                    Collectors.toMap(key.andThen(asString), Entry::getValue)
                            );

                }
        );
    }
}
