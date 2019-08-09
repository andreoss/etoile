package org.sdf.etoile;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class ReplacementMap extends
        SerializableMapEnvelop<Type, Map<Object, Object>> {
    private static final Pattern SEP = Pattern.compile("\\s*,\\s*");

    ReplacementMap(final String param) {
        super(mkMap(param));
    }

    private static Map<Type, Map<Object, Object>> mkMap(final String param) {
        final Map<Type, Map<Object, Object>> result;
        if (param == null) {
            result = Collections.emptyMap();
        } else {
            final Map<Type, Map<Object, Object>> mutable = new HashMap<>();
            final List<String> subs = split(param);
            for (final String sub : subs) {
                final String[] elems = sub.split(":", 2);
                if (elems.length != 2) {
                    throw new IllegalArgumentException(
                            String.format("`%s` is invalid", sub)
                    );
                }
                final String[] pair = elems[1].split("/", 2);
                if (pair.length != 2) {
                    throw new IllegalArgumentException(
                            String.format("`%s` is invalid", sub)
                    );
                }
                final Type key = new Type.Of(elems[0]);
                final Map<Object, Object> m = mutable
                        .computeIfAbsent(key, s -> new HashMap<>());
                if (key.klass()
                        .equals(String.class)) {
                    m
                            .put(
                                    parseNullable(String::valueOf, 0, pair),
                                    parseNullable(String::valueOf, 1, pair)
                            );
                } else if (key.klass()
                        .equals(Timestamp.class)) {
                    m
                            .put(
                                    parseNullable(Timestamp::valueOf, 0, pair),
                                    parseNullable(Timestamp::valueOf, 1, pair));
                } else {
                    throw new UnsupportedOperationException(sub);
                }
            }
            result = mutable;
        }
        return result;
    }

    private static List<String> split(final String param) {
        return Arrays.stream(SEP.split(param.trim()))
                .collect(Collectors.toList());
    }

    private static <T> T parseNullable(
            final Function<String, T> parse,
            final int i, final String[] pair
    ) {
        nullifyString(pair, i);
        @Nullable final T result;
        final String s = pair[i];
        if (null != s) {
            result = parse.apply(s);
        } else {
            result = null;
        }
        return result;
    }

    @SuppressWarnings("AssignmentToNull")
    private static void nullifyString(final String[] arr, final int i) {
        if ("null".equalsIgnoreCase(arr[i])) {
            arr[i] = null;
        }
    }

}
