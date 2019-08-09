package org.sdf.etoile;

import org.llorllale.cactoos.matchers.TextIs;
import org.llorllale.cactoos.matchers.TextMatcherEnvelope;

final class LinesAre extends TextMatcherEnvelope {
    LinesAre(final String... lines) {
        super(new TextIs(String.join(System.lineSeparator(), lines) + System.lineSeparator()), "Lines are: ");
    }
}
