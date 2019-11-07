/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import org.llorllale.cactoos.matchers.TextIs;
import org.llorllale.cactoos.matchers.TextMatcherEnvelope;

/**
 * Matcher for lines.
 *
 * @since 0.3.1
 */
final class LinesAre extends TextMatcherEnvelope {
    /**
     * Ctor.
     * @param lines Lines to match.
     */
    LinesAre(final String... lines) {
        super(
            new TextIs(
                String.join(System.lineSeparator(), lines)
                    + System.lineSeparator()
            ),
            "Lines are: "
        );
    }
}

