/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.sdf.etoile.discrepancy.Compare;
import org.sdf.etoile.discrepancy.EqualsComparison;

/**
 * Test for {@link Discrepancy}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class DiscrepancyTest extends SparkTestTemplate {

    @Test
    void shouldBeEmptyIfDatasetsMatch() {
        final Transformation<Row> trns = new Discrepancy(
            new ListOf<>("id"), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"},
            new Object[]{2, "BAR"}
        ), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"},
            new Object[]{2, "BAR"}
        ), new Compare(new EqualsComparison())
        );
        MatcherAssert.assertThat(
            "must be empty when matches",
            trns,
            new IsEmpty<>()
        );
    }

    @Test
    void whenRowMissing() {
        final Transformation<Row> trns = new Discrepancy(
            new ListOf<>("id"), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"}
        ), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"},
            new Object[]{2, "BAR"}
        ), new Compare(new EqualsComparison())
        );
        MatcherAssert.assertThat(
            "must be empty when matches",
            trns,
            new HasRows<>(
                "[2,BAR,left side is missing]"
            )
        );
    }

    @Test
    void shouldContainMismatchedRow() {
        final Transformation<Row> trns = new Discrepancy(
            new ListOf<>("id"), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"},
            new Object[]{2, "BAR"}
        ), new FakeInput(
            SparkTestTemplate.session,
            "id int, val string",
            new Object[]{1, "FOO"},
            new Object[]{2, "YOLO"}
        ), new Compare(new EqualsComparison())
        );
        MatcherAssert.assertThat(
            "must be empty when matches",
            trns,
            new HasRows<>(
                Matchers.hasToString(
                    Matchers.containsString("is \"BAR\" => was \"YOLO\"")
                )
            )
        );
    }
}
