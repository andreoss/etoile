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
 * Matcher for a positive outcome.
 *
 * @since 0.7.0
 * @todo 30m Extract common class from IsMismatch & IsOkay
 */
@RequiredArgsConstructor
public final class IsOkay extends TypeSafeMatcher<Outcome> {
    /**
     * Matcher for description.
     */
    private final Matcher<?> descr;

    /**
     * Ctor.
     * @param description Expected description.
     */
    public IsOkay(final String description) {
        this(Matchers.is(description));
    }

    /**
     * Ctor.
     */
    public IsOkay() {
        this(Matchers.notNullValue());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("should be okay")
            .appendText(" and ")
            .appendDescriptionOf(this.descr);
    }

    @Override
    public boolean matchesSafely(final Outcome item) {
        return item.isOkay() && this.descr.matches(item.description());
    }
}
