/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ValidatePartitions}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class ValidatePartitionsTest extends SparkTestTemplate {
    /**
     * Files for tests.
     */
    private final TestFiles data = new TempFiles(this.temp);

    @Test
    void validatesPartitions() {
        this.data.writeInputWithDirectory(
            "part=1",
            "1,tyu",
            "0,ghj"
        );
        this.data.writeInputWithDirectory(
            "part=0",
            "2,???",
            "4,wtf"
        );
        new ValidatePartitions(
            this.session,
            new MapOf<>(
                new MapEntry<>("expression.1", "cast(_c0 % 2 as int) as part"),
                new MapEntry<>("input.format", "csv"),
                new MapEntry<>("input.path", this.data.input().toString()),
                new MapEntry<>("output.path", this.data.output().toString())
            )
        ).run();
        MatcherAssert.assertThat(
            this.data.outputLines(),
            Matchers.contains(
                Matchers.hasToString(
                    Matchers.containsString(
                        "part=1"
                    )
                )
            )
        );
    }

    @Test
    void validatesPartitionsEmptyResultForValid() {
        this.data.writeInputWithDirectory(
            "part=1",
            "1,tyu",
            "3,ghj"
        );
        this.data.writeInputWithDirectory(
            "part=0",
            "2,???",
            "4,wtf"
        );
        new ValidatePartitions(
            this.session,
            new MapOf<>(
                new MapEntry<>("expression.1", "cast(_c0 % 2 as int) as part"),
                new MapEntry<>("input.format", "csv"),
                new MapEntry<>("input.path", this.data.input().toString()),
                new MapEntry<>("output.path", this.data.output().toString())
            )
        ).run();
        MatcherAssert.assertThat(
            this.data.outputLines(),
            Matchers.empty()
        );
    }
}
