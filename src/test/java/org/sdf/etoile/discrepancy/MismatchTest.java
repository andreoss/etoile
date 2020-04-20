/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.cactoos.list.ListOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.sdf.etoile.Compare;
import org.sdf.etoile.TestRow;
import scala.Tuple2;

/**
 * Test for {@link Mismatch}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class MismatchTest {
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
}
