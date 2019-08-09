package org.sdf.etoile;

import org.cactoos.collection.CollectionEnvelope;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class Pairs extends CollectionEnvelope<Map<String, String>> {

    Pairs(final String colon, final String sep, final String cast) {
        super(() ->
                Stream.of(cast.split(colon))
                        .filter(s -> !s.isEmpty())
                        .map(s -> Arrays.asList(s.split(sep)))
                        .map(l -> Collections.singletonMap(l.get(0), l.get(1)))
                        .collect(Collectors.toList())
        );
    }

    private Pairs(final String expressions) {
        this(",", ":", expressions);
    }

    Pairs(final String key, final Map<String, String> params) {
        this(params.getOrDefault(key, ""));
    }

}
