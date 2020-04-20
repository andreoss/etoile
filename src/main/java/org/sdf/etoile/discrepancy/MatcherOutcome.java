/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * Outcome of a matcher.
 *
 * @since 0.7.0
 */
public final class MatcherOutcome extends OutcomeEnvelope {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 7612287348168128067L;

    /**
     * Ctor.
     * @param matcher Matcher.
     * @param value Value to test agains.
     * @param <X> Type of a value.
     */
    public <X> MatcherOutcome(final Matcher<X> matcher, final X value) {
        super(
            () -> {
                final Outcome result;
                if (matcher.matches(value)) {
                    result = new Okay();
                } else {
                    final Description desc = new StringDescription();
                    desc.appendDescriptionOf(matcher);
                    desc.appendText(" => ");
                    matcher.describeMismatch(value, desc);
                    result = new Mismatch(desc.toString());
                }
                return result;
            }
        );
    }

}
