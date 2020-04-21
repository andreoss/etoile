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
 * Test for {@link DiscrepancyMain}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class DiscrepancyMainTest extends SparkTestTemplate {
    /**
     * Files for right.
     */
    private final TestFiles right = new TempFiles(this.temp);

    /**
     * Files for left.
     */
    private final TestFiles left = new TempFiles(this.temp);

    @Test
    void writesDifferenceWhenNoKeyOnRightSide() {
        this.right.writeInput(
            "1,0,wtf"
        );
        this.left.writeInput(
            "0,0,???",
            "1,0,wtf"
        );
        new DiscrepancyMain(
            SparkTestTemplate.session,
            new MapOf<>(
                new MapEntry<>("keys", "_c0"),
                new MapEntry<>("left.format", "csv"),
                new MapEntry<>("right.format", "csv"),
                new MapEntry<>("left.path", this.left.input().toString()),
                new MapEntry<>("right.path", this.right.input().toString()),
                new MapEntry<>("output.path", this.right.output().toString())
            )
        ).run();
        MatcherAssert.assertThat(
            this.right.outputLines(),
            Matchers.contains(
                "0,0,???,left side is missing"
            )
        );
    }

    @Test
    void writesDifference() {
        this.right.writeInput(
            "0,0,XXX",
            "1,0,wtf"
        );
        this.left.writeInput(
            "0,0,???",
            "1,0,wtf"
        );
        new DiscrepancyMain(
            SparkTestTemplate.session,
            new MapOf<>(
                new MapEntry<>("keys", "_c0"),
                new MapEntry<>("left.format", "csv"),
                new MapEntry<>("right.format", "csv"),
                new MapEntry<>("left.path", this.left.input().toString()),
                new MapEntry<>("right.path", this.right.input().toString()),
                new MapEntry<>("output.path", this.right.output().toString())
            )
        ).run();
        MatcherAssert.assertThat(
            this.right.outputLines(),
            Matchers.contains(
                "0,0,XXX,\"Failure(1/4): _c2(STRING):: is \\\"XXX\\\" => was \\\"???\\\"\""
            )
        );
    }

}
