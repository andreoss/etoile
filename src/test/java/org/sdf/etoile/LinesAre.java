/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.cactoos.Text;
import org.cactoos.iterable.Mapped;
import org.cactoos.text.FormattedText;
import org.cactoos.text.Joined;
import org.cactoos.text.TextOf;
import org.llorllale.cactoos.matchers.MatcherEnvelope;
import org.llorllale.cactoos.matchers.TextIs;

/**
 * Matcher for lines.
 *
 * @since 0.3.1
 */
final class LinesAre extends MatcherEnvelope<Text> {
    /**
     * Ctor.
     * @param lines Lines to match.
     */
    LinesAre(final String... lines) {
        super(
            new TextIs(
                new FormattedText(
                    "%s%n",
                    new Joined(
                        new FormattedText("%n"), new Mapped<>(TextOf::new, lines)
                    )
                )
            )
        );
    }
}

