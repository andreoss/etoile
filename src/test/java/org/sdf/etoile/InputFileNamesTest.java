/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.List;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.object.HasToString;
import org.junit.jupiter.api.Test;

/**
 * Test for @{link Files}.
 *
 * @since 0.6.0
 */
final class InputFileNamesTest extends SparkTestTemplate {

    @Test
    void addsFilenamesAsColumn() {
        final String[] lines = {
            "0,abc",
            "1,xyz",
        };
        this.data.writeInput(
            "id,name",
            lines[0],
            lines[1]
        );
        final List<Row> result = new Collected<>(
            new InputFileNames<>(
                "__file",
                new CsvInput(this.session(), this.data.input())
            )
        );
        MatcherAssert.assertThat(
            "should add a column with file name",
            result,
            new IsIterableContainingInOrder<>(
                new ListOf<>(
                    new HasToString<>(
                        Matchers.startsWith("[0,abc,file:///")
                    ),
                    new HasToString<>(
                        Matchers.startsWith("[1,xyz,file:///")
                    )
                )
            )
        );
    }

}

