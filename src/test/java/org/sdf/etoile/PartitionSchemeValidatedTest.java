/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Row;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.sdf.etoile.expr.ExpressionOf;

/**
 * Test for {@link PartitionSchemeValidated}.
 *
 * @since 0.6.0
 * @checkstyle ClassDataAbstractionCouplingCheck (100 lines)
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class PartitionSchemeValidatedTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @Test
    void outputsMismatchRows() {
        final String[] lines = {
            "123,gts",
            "321,bar",
            "0,foo",
        };
        this.data.writeInput(
            "nm,ss",
            lines[0],
            lines[1]
        );
        final URI result = new Saved<>(
            this.data.output().toPath(),
            new PartitionedBy<>(
                "part_id",
                new FormatOutput<>(
                    new Repartitioned<>(
                        new WithColumns(
                            new CsvInput(this.session, this.data.input()),
                            new ExpressionOf("pmod(nm, 100) as part_id")
                        ),
                        new ExpressionOf("part_id")
                    ),
                    Collections.singletonMap("format", "json")
                )
            )
        ).result();
        final Transformation<Row> input =
            new PartitionSchemeValidated(
                new Input(
                    this.session,
                    new MapOf<>(
                        new MapEntry<>("path", result.toString()),
                        new MapEntry<>("format", "json")
                    )
                ),
                new ListOf<>(new ExpressionOf("pmod(nm, 101) as part_id"))
            );
        final List<Row> rows = new Collected<>(
            input
        );
        MatcherAssert.assertThat(
            "output must contain mismatches",
            rows,
            Matchers.containsInAnyOrder(
                Matchers.hasToString(Matchers.containsString(lines[0])),
                Matchers.hasToString(Matchers.containsString(lines[1]))
            )
        );
    }

    @Test
    void validatesEmptyOutputIfMatch() {
        final String[] lines = {
            "0,abc",
            "1,xyz",
        };
        this.data.writeInput(
            "id,name",
            lines[0],
            lines[1]
        );
        final URI result = new Saved<>(
            this.data.output().toPath(),
            new PartitionedBy<>(
                "id",
                new FormatOutput<>(
                    new Repartitioned<>(
                        new CsvInput(this.session, this.data.input()),
                        new ExpressionOf("id")
                    ),
                    Collections.singletonMap("format", "json")
                )
            )
        ).result();
        final Transformation<Row> input =
            new PartitionSchemeValidated(
                new Input(
                    this.session,
                    new MapOf<>(
                        new MapEntry<>("path", result.toString()),
                        new MapEntry<>("format", "json")
                    )
                ),
                new ExpressionOf("id as id")
            );
        final List<Row> rows = new Collected<>(
            input
        );
        MatcherAssert.assertThat(
            "should be empty when all partition match",
            rows,
            Matchers.empty()
        );
    }

}

