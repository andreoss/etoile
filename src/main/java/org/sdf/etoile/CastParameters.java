package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
final class CastParameters implements List<Map<String, String>> {
    private final String colon;
    private final String sep;
    private final String cast;

    private CastParameters(final String cast) {
        this(",", ":", cast);
    }

    CastParameters(final String key, final Map<String, String> params) {
        this(params.getOrDefault(key, ""));
    }

    @Delegate
    private List<Map<String, String>> parseCastParameter() {
        return Stream.of(cast.split(colon))
                .filter(s -> !s.isEmpty())
                .map(s -> Arrays.asList(s.split(sep)))
                .map(l -> Collections.singletonMap(l.get(0), l.get(1)))
                .collect(Collectors.toList());
    }
}
