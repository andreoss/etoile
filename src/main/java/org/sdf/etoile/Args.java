package org.sdf.etoile;

import org.cactoos.map.MapEnvelope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Args extends MapEnvelope<String, String> {
    private static final Pattern ARG_RX = Pattern.compile(
            "--(?<key>[a-z0-9-\\.]+)=(?<value>.+)"
    );

    Args(final Pattern pattern, final String... arguments) {
        super(() -> get(pattern, arguments));
    }

    Args(final String... arguments) {
        this(ARG_RX, arguments);
    }

    private static Map<String, String> get(
            final Pattern pattern,
            final String[] args
    ) {
        final Map<String, String> result = new HashMap<>();
        for (final String arg : args) {
            final Matcher matcher = pattern.matcher(arg);
            if (matcher.matches()) {
                result.put(
                        matcher.group("key"),
                        matcher.group("value")
                );
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "%s does not match %s",
                                arg,
                                pattern
                        )
                );
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
