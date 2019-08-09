package org.sdf.etoile;


import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

import java.sql.Timestamp;

public final class ReplacementMapTest {

    @Test(expected = IllegalArgumentException.class)
    public void failsOnInvalidInput() {
        new ReplacementMap("string:XXX:WTF");
    }

    @Test(expected = IllegalArgumentException.class)
    public void failsOnInvalidInput2() {
        new ReplacementMap("string:XXX");
    }


    @Test
    public void buildsMapForStringAndTrimsSeveralKeys() {
        MatcherAssert.assertThat(
                "trims string and creates map",
                new ReplacementMap(
                        "  string:XXX/MISSING, string:YYY/MISSING "
                ),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        Matchers.allOf(
                                IsMapContaining.hasEntry(
                                        "YYY", "MISSING"
                                ),
                                IsMapContaining.hasEntry(
                                        "XXX", "MISSING"
                                )
                        )
                )
        );
    }

    @Test
    public void buildsMapForStringAndTrims() {
        MatcherAssert.assertThat(
                "trims string and creates map",
                new ReplacementMap("  string:XXX/MISSING    "),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        IsMapContaining.hasEntry(
                                "XXX", "MISSING"
                        )
                )
        );
    }

    @Test
    public void buildsMapForString() {
        MatcherAssert.assertThat(
                "creates map",
                new ReplacementMap("string:XXX/MISSING"),
                Matchers.hasEntry(
                        Matchers.is(new Type.Of("string")),
                        IsMapContaining.hasEntry(
                                "XXX", "MISSING"
                        )
                )
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void throwsOnUnsupportedType() {
        new ReplacementMap("decimal:-1/0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsOnMalformedString() {
        new ReplacementMap("decimal->-1/0");
    }

    @Test
    public void handlesNulls() {
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
    public void buildsMapForTimestamp() {
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
