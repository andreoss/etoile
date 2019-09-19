package org.sdf.etoile;

import org.apache.spark.sql.types.StringType$;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

final class TypeTest {
    @Test
    void convertsDatatypeToJavaTypeDatatype() {
        MatcherAssert.assertThat(
                "string is String",
                new Type.Of("string").value(),
                Matchers.is(StringType$.MODULE$)
        );
    }

    @Test
    void shouldNotBeequalWithDifferentType() {
        MatcherAssert.assertThat(
                "not equals",
                new Type.Of(StringType$.MODULE$),
                Matchers.not(new Type.Of("timestamp"))
        );
    }

    @Test
    void shouldNotBeEqualWithDifferentObject() {
        MatcherAssert.assertThat(
                "not equals to ???",
                new Type.Of(StringType$.MODULE$),
                Matchers.not(Matchers.equalTo(new Object()))
        );
    }

    @Test
    void equalsWithSameType() {
        MatcherAssert.assertThat(
                "equals",
                new Type.Of(StringType$.MODULE$),
                Matchers.is(new Type.Of("string"))
        );
    }

    @Test
    void cannotParseUnknownType() {
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                new Type.Of("struct<a:string>")::klass
        );
    }

    @Test
    void hasToSting() {
        MatcherAssert.assertThat(
                new Type.Of("struct<a:string>"),
                Matchers.hasToString(
                        "Type.Of(dataType=StructType(StructField(a,StringType,true)))"
                )
        );
    }


    @Test
    void parsesTimestamp() {
        MatcherAssert.assertThat(
                "timestamp is Timestamp",
                new Type.Of("timestamp").klass(),
                Matchers.is(Timestamp.class)
        );
    }

    @Test
    void convertsDatatypeToJavaTypeForString() {
        MatcherAssert.assertThat(
                "string is String",
                new Type.Of("string").klass(),
                Matchers.is(String.class)
        );
    }

    @Test
    void convertsDatatypeToJavaTypeForTimestamp() {
        MatcherAssert.assertThat(
                "timestamp is Timestamp",
                new Type.Of("timestamp").klass(),
                Matchers.is(Timestamp.class)
        );
    }
}