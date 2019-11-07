/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.sql.Timestamp;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ReplacementMap}.
 *
 * @since 0.2.5
 */
final class ReplacementMapTest {

    /**
     * String type.
     */
    public static final TypeOf STRING = new TypeOf("string");

    /**
     * Timestamp type.
     */
    private static final Type TIMESTAMP = new TypeOf("timestamp");

    /**
     * Some date.
     */
    private static final Timestamp TMSTM = Timestamp.valueOf(
        "1999-01-01 00:00:00.0"
    );

    @Test
    void failsOnInvalidInput() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new ReplacementMap("string:XXX:WTF")
        );
    }

    @Test
    void failsOnInvalidInputNoReplacement() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new ReplacementMap("string:XXX")
        );
    }

    @Test
    void buildsMapForStringAndTrimsSeveralKeys() {
        MatcherAssert.assertThat(
            "trims string and creates map",
            new ReplacementMap(
                "  string:XXX/YOLO, string:YYY/BAR "
            ),
            Matchers.hasEntry(
                Matchers.is(ReplacementMapTest.STRING),
                Matchers.allOf(
                    IsMapContaining.hasEntry(
                        "YYY", "BAR"
                    ),
                    IsMapContaining.hasEntry(
                        "XXX", "YOLO"
                    )
                )
            )
        );
    }

    @Test
    void buildsMapForStringAndTrims() {
        MatcherAssert.assertThat(
            "trims string from both sides",
            new ReplacementMap("  string:ZZZ/DEFAULT    "),
            Matchers.hasEntry(
                Matchers.is(ReplacementMapTest.STRING),
                IsMapContaining.hasEntry(
                    "ZZZ", "DEFAULT"
                )
            )
        );
    }

    @Test
    void buildsMapForString() {
        MatcherAssert.assertThat(
            "creates map for single replecement",
            new ReplacementMap("string:QQQ/MISSING"),
            Matchers.hasEntry(
                Matchers.is(ReplacementMapTest.STRING),
                IsMapContaining.hasEntry(
                    "QQQ", "MISSING"
                )
            )
        );
    }

    @Test
    void throwsOnUnsupportedType() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new ReplacementMap("decimal:-1/0")
        );
    }

    @Test
    void throwsOnMalformedString() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new ReplacementMap("decimal->-1/0")
        );
    }

    @Test
    void handlesNulls() {
        MatcherAssert.assertThat(
            "creates map for null",
            new ReplacementMap("timestamp:null/1999-01-01 00:00:00"),
            Matchers.hasEntry(
                Matchers.is(ReplacementMapTest.TIMESTAMP),
                IsMapContaining.hasEntry(
                    Matchers.nullValue(),
                    Matchers.equalTo(ReplacementMapTest.TMSTM)
                )
            )
        );
    }

    @Test
    void buildsMapForTimestamp() {
        MatcherAssert.assertThat(
            "creates map replecement to null",
            new ReplacementMap("timestamp:1999-01-01 00:00:00/null"),
            Matchers.hasEntry(
                Matchers.is(ReplacementMapTest.TIMESTAMP),
                IsMapContaining.hasEntry(
                    Matchers.equalTo(ReplacementMapTest.TMSTM),
                    Matchers.nullValue()
                )
            )
        );
    }
}
