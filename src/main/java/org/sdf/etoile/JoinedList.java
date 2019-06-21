package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
final class JoinedList<K> implements List<K> {
    private final List<List<K>> lists;

    @SafeVarargs
    JoinedList(final List<K>... xs) {
        this(Arrays.asList(xs));
    }

    @Delegate
    private List<K> value() {
        return lists.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
