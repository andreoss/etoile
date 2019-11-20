/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.cactoos.map.MapEnvelope;

/**
 * Command-line arguments.
 *
 * @since 0.1.0
 * @todo Replace with args4j
 */
final class Args extends MapEnvelope<String, String> {
    /**
     * Secondary ctor.
     *
     * @param arguments Command-line argumnets.
     */
    Args(final String... arguments) {
        this(
            Pattern.compile("--(?<key>[a-z0-9-\\.]+)=(?<value>.+)"), arguments
        );
    }

    /**
     * Ctor.
     * @param pattern Parse pattern.
     * @param arguments Command-line arguments.
     */
    Args(final Pattern pattern, final String... arguments) {
        super(
            () -> {
                final Map<String, String> result = new HashMap<>();
                for (final String arg : arguments) {
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
        );
    }

}
