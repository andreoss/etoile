package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
final class Args implements Map<String, String> {
    private static final Pattern ARG_RX = Pattern.compile(
            "--(?<key>[a-z0-9-\\.]+)=(?<value>.+)"
    );

    private final Pattern pattern;
    private final String[] args;

    Args(final String... arguments) {
        this(ARG_RX, arguments);
    }

    @Delegate
    private Map<String, String> get() {
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
