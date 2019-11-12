package org.sdf.etoile;


import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

final class ReplacementMapTest {

    @Test
    void failsOnInvalidInput() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new ReplacementMap("string:XXX:WTF")
        );
    }

    @Test
    void failsOnInvalidInput2() {
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
                        "  string:XXX/DEFAULT_VALUE, string:YYY/DEFAULT_VALUE "
                ),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        Matchers.allOf(
                                IsMapContaining.hasEntry(
                                        "YYY", "DEFAULT_VALUE"
                                ),
                                IsMapContaining.hasEntry(
                                        "XXX", "DEFAULT_VALUE"
                                )
                        )
                )
        );
    }

    @Test
    void buildsMapForStringAndTrims() {
        MatcherAssert.assertThat(
                "trims string and creates map",
                new ReplacementMap("  string:XXX/DEFAULT_VALUE    "),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        IsMapContaining.hasEntry(
                                "XXX", "DEFAULT_VALUE"
                        )
                )
        );
    }

    @Test
    void buildsMapForString() {
        MatcherAssert.assertThat(
                "creates map",
                new ReplacementMap("string:XXX/DEFAULT_VALUE"),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        IsMapContaining.hasEntry(
                                "XXX", "DEFAULT_VALUE"
                        )
                )
        );
    }

    @Test
    void throwsOnUnsupportedType() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> new ReplacementMap("decimal:-1/0"));
    }

    @Test
    void throwsOnMalformedString() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ReplacementMap("decimal->-1/0"));
    }

    @Test
    void handlesNulls() {
        MatcherAssert.assertThat(
                "creates map",
                new ReplacementMap("timestamp:null/1999-01-01 00:00:00"),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("timestamp")),
                        IsMapContaining.hasEntry(
                                Matchers.nullValue(),
                                Matchers.equalTo(Timestamp.valueOf("1999-01-01 00:00:00.0"))
                        )
                )
        );
    }

    @Test
    void buildsMapForTimestamp() {
        MatcherAssert.assertThat(
                "creates map",
                new ReplacementMap("timestamp:1999-01-01 00:00:00/null"),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("timestamp")),
                        IsMapContaining.hasEntry(
                                Matchers.equalTo(Timestamp.valueOf("1999-01-01 00:00:00.0")),
                                Matchers.nullValue()
                        )
                )
        );
    }
}
