/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.Matchers;

/**
 * Equality comparison.
 *
 * @since 0.7.0
 */
public final class EqualsComparison implements Comparison {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 2166105011179914395L;

    @Override
    public Outcome make(final Object right, final Object left) {
        return new MatcherOutcome(Matchers.is(right), left);
    }
}
