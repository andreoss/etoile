package org.sdf.etoile;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class ReplacementMap extends
        SerializableMapEnvelop<Type, Map<Object, Object>> {
    private static final Pattern SEP = Pattern.compile("\\s*,\\s*");

    ReplacementMap(final String param) {
        super(build(param));
    }

    private static Map<Type, Map<Object, Object>> build(final String param) {
        final Map<Type, Map<Object, Object>> result;
        if (param == null) {
            result = Collections.emptyMap();
        } else {
            final Map<Type, Map<Object, Object>> mutable = new HashMap<>();
            final List<String> subs = Arrays.stream(SEP.split(param))
                    .filter(x -> !x.isEmpty())
                    .collect(Collectors.toList());
            for (final String sub : subs) {
                final String[] elems = sub.split(":", 2);
                final String[] pair = elems[1].split("/", 2);
                check(sub, elems, pair);
                parseNull(pair, 0);
                parseNull(pair, 1);
                final Type key = new Type.Of(elems[0]);
                if (key.klass()
                        .equals(String.class)) {
                    mutable.computeIfAbsent(key, s -> new HashMap<>())
                            .put(pair[0], pair[1]);
                } else if (key.klass()
                        .equals(Timestamp.class)) {
                    @Nullable final Timestamp value;
                    if (pair[1] != null) {
                        value = Timestamp.valueOf(pair[1]);
                    } else {
                        value = null;
                    }
                    mutable.computeIfAbsent(key, s -> new HashMap<>())
                            .put(Timestamp.valueOf(pair[0]), value);
                } else {
                    throw new UnsupportedOperationException(sub);
                }
            }
            result = mutable;
        }
        return result;
    }

    @SuppressWarnings("AssignmentToNull")
    private static void parseNull(final String[] repl, final int i) {
        if ("null".equals(repl[i])) {
            repl[i] = null;
        }
    }

    private static void check(
            final String x,
            final String[] elems,
            final String[] pair
    ) {
        if ((elems.length != 2) || (pair.length != 2)) {
            throw new IllegalArgumentException(
                    String.format("`%s` is invalid", x)
            );
        }
    }

}
