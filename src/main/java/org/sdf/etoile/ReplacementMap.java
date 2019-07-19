package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RequiredArgsConstructor
final class ReplacementMap implements Map<Type, Map<Object, Object>>, Serializable {
    private static final Pattern SEP = Pattern.compile("\\s*,\\s*");
    private final String param;

    @Nullable
    private Map<Type, Map<Object, Object>> res;

    @Delegate
    private Map<Type, Map<Object, Object>> make() {
        if (res == null) {
            build();
        }
        return res;
    }

    private void build() {
        final Map<Type, Map<Object, Object>> mutable = new HashMap<>();
        final List<String> subs = Arrays.stream(SEP.split(param))
                .filter(x -> !x.isEmpty())
                .collect(Collectors.toList());
        for (final String sub : subs) {
            final String[] elems = sub.split(":", 2);
            final String[] repl = elems[1].split("/", 2);
            check(sub, elems, repl);
            repl[0] = repl[0].equals("null") ? null : repl[0];
            repl[1] = repl[1].equals("null") ? null : repl[1];
            final Type key = new Type.Of(elems[0]);
            if (key.klass()
                    .equals(String.class)) {
                mutable.computeIfAbsent(key, s -> new HashMap<>())
                        .put(repl[0], repl[1]);
            } else if (key.klass()
                    .equals(Timestamp.class)) {
                final Timestamp value = repl[1] != null ? Timestamp.valueOf(repl[1]) : null;
                mutable.computeIfAbsent(key, s -> new HashMap<>())
                        .put(Timestamp.valueOf(repl[0]), value);
            } else {
                throw new UnsupportedOperationException(sub);
            }
        }
        res = Collections.unmodifiableMap(mutable);
    }

    private void check(final String x, final String[] elems, final String[] repl) {
        if ((elems.length != 2) || (repl.length != 2)) {
            throw new IllegalArgumentException(
                    String.format("`%s` contains `%s`", param, x)
            );
        }
    }
}
