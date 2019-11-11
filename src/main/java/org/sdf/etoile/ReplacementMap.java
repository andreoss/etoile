/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Replacement dictionary by type.
 *
 * @since 0.2.5
 * @todo Refactor to a simplier interface.
 */
final class ReplacementMap extends
    SerializableMapEnvelope<Type, Map<Object, Object>> {
    /**
     * Separator.
     */
    private static final Pattern SEP = Pattern.compile("\\s*,\\s*");

    /**
     * Ctor.
     * @param param Comma-separated parameter string.
     */
    ReplacementMap(final String param) {
        super(ReplacementMap.mkMap(param));
    }

    /**
     * Build dictionary.
     * @param param Command-line parameter.
     * @return Replecement map.
     */
    private static Map<Type, Map<Object, Object>> mkMap(final String param) {
        final Map<Type, Map<Object, Object>> result;
        if (param == null) {
            result = Collections.emptyMap();
        } else {
            final Map<Type, Map<Object, Object>> mutable = new HashMap<>();
            final List<String> subs = ReplacementMap.split(param);
            for (final String sub : subs) {
                final String[] elems = sub.split(":", 2);
                final String[] pair = ReplacementMap.parseToPair(sub, elems);
                final Type key = new TypeOf(elems[0]);
                final Map<Object, Object> rplc = mutable.computeIfAbsent(
                    key, s -> new HashMap<>()
                );
                if (key.asJava().equals(String.class)) {
                    rplc.put(
                        ReplacementMap.parseNullable(String::valueOf, 0, pair),
                        ReplacementMap.parseNullable(String::valueOf, 1, pair)
                    );
                } else if (key.asJava().equals(Timestamp.class)) {
                    rplc.put(
                        ReplacementMap.parseNullable(Timestamp::valueOf, 0, pair),
                        ReplacementMap.parseNullable(Timestamp::valueOf, 1, pair)
                    );
                } else {
                    throw new UnsupportedOperationException(sub);
                }
            }
            result = mutable;
        }
        return result;
    }

    /**
     * Parse string to pair.
     * @param sub Original string for Exception.
     * @param elems Separated by colon.
     * @return A pair.
     */
    private static String[] parseToPair(final String sub, final String... elems) {
        final RuntimeException exc = new IllegalArgumentException(
            String.format("`%s` is invalid", sub)
        );
        if (elems.length != 2) {
            throw exc;
        }
        final String[] pair = elems[1].split("/", 2);
        if (pair.length != 2) {
            throw exc;
        }
        return pair;
    }

    /**
     * Split & trim a string.
     * @param param String to split
     * @return List of strings.
     */
    private static List<String> split(final String param) {
        return Arrays.stream(ReplacementMap.SEP.split(param.trim()))
            .collect(Collectors.toList());
    }

    /**
     * Replace literal "null" with {@code null} and parse.
     * @param parse Parse function.
     * @param id Element id.
     * @param pair Pair to process.
     * @param <T> Type of parsing result.
     * @return Parsing result.
     */
    private static <T> T parseNullable(
        final Function<String, T> parse,
        final int id, final String... pair
    ) {
        ReplacementMap.nullifyString(pair, id);
        @Nullable final T result;
        final String elem = pair[id];
        if (elem == null) {
            result = null;
        } else {
            result = parse.apply(elem);
        }
        return result;
    }

    /**
     * Nullify elements with liternal "null".
     * @param arr Array of strings.
     * @param id Element id to process.
     */
    private static void nullifyString(final String[] arr, final int id) {
        if ("null".equalsIgnoreCase(arr[id])) {
            arr[id] = null;
        }
    }
}
