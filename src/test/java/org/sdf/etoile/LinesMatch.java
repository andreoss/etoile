/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.cactoos.text.Joined;
import org.llorllale.cactoos.matchers.MatchesRegex;
import org.llorllale.cactoos.matchers.TextMatcherEnvelope;

/**
 * Match lines with regex.
 *
 * @since 0.6.0
 */
public final class LinesMatch extends TextMatcherEnvelope {
    /**
     * Ctor.
     *
     * @param rxs Regexes for each line.
     */
    public LinesMatch(final String... rxs) {
        super(
            new MatchesRegex(
                new Joined("\n", rxs)
            ),
            "lines should match:"
        );
    }
}
