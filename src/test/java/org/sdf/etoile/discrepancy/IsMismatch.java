/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import lombok.RequiredArgsConstructor;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for a negative outcome.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
public final class IsMismatch extends TypeSafeMatcher<Outcome> {
    /**
     * Matcher for description.
     */
    private final Matcher<?> descr;

    /**
     * Ctor.
     */
    public IsMismatch() {
        this(Matchers.notNullValue());
    }

    /**
     * Ctor.
     * @param description Expected description.
     */
    public IsMismatch(final String description) {
        this(Matchers.is(description));
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("should be not okay")
            .appendText(" and ")
            .appendDescriptionOf(this.descr);
    }

    @Override
    public boolean matchesSafely(final Outcome item) {
        return item.isNotOkay() && this.descr.matches(item.description());
    }
}

