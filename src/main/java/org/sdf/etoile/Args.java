package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
public final class Args implements Supplier<Map<String, String>>, Map<String, String> {
    private static final Pattern ARG_RX = Pattern.compile("--(?<key>\\w+)=(?<value>\\S+)");

    private final Pattern pattern;
    private final String[] args;

    public Args(final String... args) {
        this(ARG_RX, args);
    }

    @Delegate
    public Map<String, String> get() {
        final Map<String, String> result = new HashMap<>();
        for (final String arg : args) {
            final Matcher matcher = pattern.matcher(arg);
            if (matcher.matches()) {
                result.put(
                        matcher.group("key"),
                        matcher.group("value")
                );
            } else {
                throw new IllegalArgumentException(arg + " does not match " + pattern);
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
