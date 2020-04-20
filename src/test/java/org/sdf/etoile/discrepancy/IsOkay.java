/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for a positive outcome.
 *
 * @since 0.7.0
 */
public final class IsOkay extends TypeSafeMatcher<Outcome> {
    @Override
    public void describeTo(final Description description) {
        description.appendText("should be okay");
    }

    @Override
    public boolean matchesSafely(final Outcome item) {
        return item.isOkay();
    }
}
