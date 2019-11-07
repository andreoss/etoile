/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.cactoos.collection.CollectionEnvelope;

/**
 * An delimited expression as a Collection of Map.
 *
 * @since 0.1.0
 */
final class Pairs extends CollectionEnvelope<Map<String, String>> {

    /**
     * Secondary ctor.
     *
     * @param key A key for parameter
     * @param params Parameters
     */
    Pairs(final String key, final Map<String, String> params) {
        this(params.getOrDefault(key, ""));
    }

    /**
     * Secondary ctor.
     *
     * @param expressions An expression string.
     */
    Pairs(final String expressions) {
        this(",", ":", expressions);
    }

    /**
     * Ctor.
     *
     * @param colon Element separator
     * @param sep Key/value separator
     * @param expr An expression string
     */
    Pairs(final String colon, final String sep, final String expr) {
        super(() -> Stream.of(expr.split(colon))
            .filter(s -> !s.isEmpty())
            .map(s -> Arrays.asList(s.split(sep)))
            .map(l -> Collections.singletonMap(l.get(0), l.get(1)))
            .collect(Collectors.toList()));
    }

}
