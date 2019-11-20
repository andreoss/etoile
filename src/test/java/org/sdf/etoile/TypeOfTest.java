/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.sql.Timestamp;
import org.apache.spark.sql.types.StringType$;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link TypeOf}.
 * @since 0.2.5
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class TypeOfTest {
    @Test
    void convertsDatatypeToJavaTypeDatatype() {
        MatcherAssert.assertThat(
            "string is String",
            new TypeOf("string").asSpark(),
            Matchers.is(StringType$.MODULE$)
        );
    }

    @Test
    void shouldNotBeequalWithDifferentType() {
        MatcherAssert.assertThat(
            "not equals",
            new TypeOf(StringType$.MODULE$),
            Matchers.not(new TypeOf("timestamp"))
        );
    }

    @Test
    void shouldNotBeEqualWithDifferentObject() {
        MatcherAssert.assertThat(
            "not equals to ???",
            new TypeOf(StringType$.MODULE$),
            Matchers.not(Matchers.equalTo(new Object()))
        );
    }

    @Test
    void equalsWithSameType() {
        MatcherAssert.assertThat(
            "equals",
            new TypeOf(StringType$.MODULE$),
            Matchers.is(new TypeOf("string"))
        );
    }

    @Test
    void cannotParseUnknownType() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            new TypeOf("struct<a:string>")::asJava
        );
    }

    @Test
    void hasToSting() {
        MatcherAssert.assertThat(
            new TypeOf("struct<a:string>"),
            Matchers.hasToString(
                "TypeOf(type=struct<a:string>)"
            )
        );
    }

    @Test
    void parsesTimestamp() {
        MatcherAssert.assertThat(
            "timestamp is Timestamp",
            new TypeOf("timestamp").asJava(),
            Matchers.is(Timestamp.class)
        );
    }

    @Test
    void convertsDatatypeToJavaTypeForString() {
        MatcherAssert.assertThat(
            "string is String",
            new TypeOf("string").asJava(),
            Matchers.is(String.class)
        );
    }

    @Test
    void convertsDatatypeToJavaTypeForTimestamp() {
        MatcherAssert.assertThat(
            "timestamp is Timestamp",
            new TypeOf("timestamp").asJava(),
            Matchers.is(Timestamp.class)
        );
    }
}
