/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for an empty {@link Transformation}.
 *
 * @param <X> Underlying datatype.
 * @since 0.7.0
 */
public final class IsEmpty<X> extends TypeSafeMatcher<Transformation<X>> {
    @Override
    public void describeTo(final Description description) {
        description.appendText("should be empty");
    }

    @Override
    public boolean matchesSafely(final Transformation<X> item) {
        return item.get().rdd().isEmpty();
    }
}
