package org.sdf.etoile;


import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

import java.sql.Timestamp;

public class ReplacementMapTest {


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
