/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.cactoos.list.ListOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.sdf.etoile.TestRow;
import scala.Tuple2;

/**
 * Test for {@link Compare}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class CompareTest {
    @Test
    void shouldReturnIfLeftIsNull() throws Exception {
        MatcherAssert.assertThat(
            new Compare(new EqualsComparison())
                .call(
                    Tuple2.apply(
                        new TestRow(
                            "id int, num int, name string, __result string",
                            1, 2, "Y", null
                        ),
                        null
                    )
                ).next(),
            Matchers.hasToString(
                Matchers.containsString("right side is missing")
            )
        );
    }

    @Test
    void throwsExceptionWhenBothAreNull() throws Exception {
        Assertions.assertThrows(
            NullPointerException.class, () ->
                new Compare(new EqualsComparison()).apply(null, null)
        );
    }

    @Test
    void shouldReturnIfRightIsNull() throws Exception {
        MatcherAssert.assertThat(
            new Compare(new EqualsComparison())
                .call(
                    Tuple2.apply(
                        null,
                        new TestRow(
                            "id int, num int, name string, __result string",
                            1, 2, "Y", null
                        )
                    )
                ).next(),
            Matchers.hasToString(
                Matchers.containsString("left side is missing")
            )
        );
    }

    @Test
    void shouldReturnEmptyIteratorIfMatch() throws Exception {
        MatcherAssert.assertThat(
            new ListOf<>(
                new Compare(new EqualsComparison())
                    .call(
                        Tuple2.apply(
                            new GenericRowWithSchema(
                                new Object[]{1, 2, "X"},
                                StructType.fromDDL("id int, num int, name string")
                            ),
                            new GenericRowWithSchema(
                                new Object[]{1, 2, "X"},
                                StructType.fromDDL("id int, num int, name string")
                            )
                        )
                    )
            ),
            Matchers.empty()
        );
    }

    @Test
    void shouldReturnRowIteratorIfDoNotMatch() throws Exception {
        MatcherAssert.assertThat(
            new Compare(new EqualsComparison())
                .call(
                    Tuple2.apply(
                        new TestRow(
                            "id int, num int, name string, __result string",
                            1, 2, "X", null
                        ),
                        new TestRow(
                            "id int, num int, name string, __result string",
                            1, 2, "Y", null
                        )
                    )
                ).next(),
            Matchers.hasToString(
                Matchers.containsString("is \"X\" => was \"Y\"")
            )
        );
    }

    @Test
    void shouldReturnIfLeftIfAllColumnsAreNow() throws Exception {
        MatcherAssert.assertThat(
            new Compare(new EqualsComparison())
                .call(
                    Tuple2.apply(
                        new TestRow(
                            "id int, num int, name string, __result string",
                            1, 2, "Y", null
                        ),
                        new TestRow(
                            "id int, num int, name string, __result string",
                            null, null, null, null
                        )
                    )
                ).next(),
            Matchers.hasToString(
                Matchers.containsString("right side is missing")
            )
        );
    }

}
