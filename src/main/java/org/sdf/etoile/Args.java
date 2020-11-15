/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.cactoos.iterable.IterableOf;
import org.cactoos.map.MapEnvelope;
import org.cactoos.map.MapOf;

/**
 * Command-line arguments.
 *
 * @since 0.1.0
 * @todo #0:24h Replace this Map with proper object for configuration.
 *  Take into account that prefix is stripped with {@link PrefixArgs} in
 *  many places.
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
            new MapOf<String, String>(
                new IterableOf<Entry<? extends String, ? extends String>>(
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
                        return Collections.unmodifiableMap(result).entrySet().iterator();
                    }
                )
            )
        );
    }

}
