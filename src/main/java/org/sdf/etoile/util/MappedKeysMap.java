package org.sdf.etoile.util;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public final class MappedKeysMap<X, K, V> implements Map<X, V> {
    private final Function<K, X> asString;
    private final Map<K, V> orig;

    @Delegate
    private Map<X, V> value() {
        final Function<Entry<K, V>, K> key = Entry::getKey;
        return orig.entrySet()
                .stream()
                .collect(
                        Collectors.toMap(key.andThen(asString), Entry::getValue)
                );
    }

}
