/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.cactoos.iterable.Mapped;
import org.cactoos.list.ListOf;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.collection.IsIterableContainingInOrder;

/**
 * Has rows.
 *
 * @since 0.7.0
 * @param <X> Type of transformation.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class HasRows<X> extends TypeSafeDiagnosingMatcher<Transformation<X>> {
    /**
     * Matcher for list of Rows.
     */
    private final Matcher<Iterable<? extends X>> matcher;

    /**
     * Ctor.
     * @param rows Rows as Strings.
     */
    public HasRows(final String... rows) {
        this(
            new Mapped<>(
                Matchers::hasToString,
                new ListOf<>(rows)
            )
        );
    }

    /**
     * Ctor.
     * @param matcher Rows in Order.
     */
    public HasRows(final Iterable<Matcher<? extends X>> matcher) {
        this(new IsIterableContainingInOrder(new ListOf<Matcher<?>>(matcher)));
    }

    /**
     * Ctor.
     * @param matchers Rows in Order.
     */
    @SafeVarargs
    public HasRows(final Matcher<? extends X>... matchers) {
        this(new ListOf<>(matchers));
    }

    @Override
    public final void describeTo(final Description description) {
        description.appendText("contains rows:").appendValue(this.matcher);
    }

    @Override
    protected final boolean matchesSafely(final Transformation<X> item,
        final Description mismatch) {
        final List<X> collected = item.get().collectAsList();
        mismatch.appendText("collected ").appendValue(collected);
        this.matcher.describeMismatch(collected, mismatch);
        return this.matcher.matches(collected);
    }
}
