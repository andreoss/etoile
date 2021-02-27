/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ConditionalTransformation}.
 * @since 0.1.0
 */
final class ConditionalTransformationTest extends SparkTestTemplate {

    /**
     * Executes lhs transformation when condition evaluates to {@code false}.
     */
    @Test
    void returnsLeftIfConditionIsFalse() {
        MatcherAssert.assertThat(
            "uses left transformation if false",
            new SchemaOf<>(
                new ConditionalTransformation<>(
                    () -> false,
                    this.left(),
                    this.right()
                )
            ).asMap(),
            Matchers.hasEntry("id", "string")
        );
    }

    /**
     * Executes rhs transformation when condition evaluates to {@code true}.
     */
    @Test
    void returnsRightIfConditionMeets() {
        MatcherAssert.assertThat(
            "uses right transformation if true",
            new SchemaOf<>(
                new ConditionalTransformation<>(
                    () -> true,
                    this.left(),
                    this.right()
                )
            ).asMap(),
            Matchers.hasEntry("id", "int")
        );
    }

    private Noop<Row> left() {
        return new Noop<>(new FakeInput(this.session(), "id int"));
    }

    private Noop<Row> right() {
        return new Noop<>(new FakeInput(this.session(), "id string"));
    }

}
