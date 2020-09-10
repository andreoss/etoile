/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link EqualsComparison}.
 *
 * @since 0.7.0
 */
final class EqualsComparisonTest {
    @Test
    void comparesByEqual() {
        MatcherAssert.assertThat(
            new EqualsComparison().make(new Object(), new Object()),
            new IsNot<>(new IsOkay())
        );
    }
}

