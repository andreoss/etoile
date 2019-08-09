package org.sdf.etoile;

import org.apache.spark.sql.types.StringType$;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.sql.Timestamp;

public final class TypeTest {
    @Test
    public void convertsDatatypeToJavaTypeDatatype() {
        MatcherAssert.assertThat(
                "string is String",
                new Type.Of("string").value(),
                Matchers.is(StringType$.MODULE$)
        );
    }

    @Test
    public void equalsWithSameType() {
        MatcherAssert.assertThat(
                "equals",
                new Type.Of(StringType$.MODULE$),
                Matchers.is(new Type.Of("string"))
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cannotParseUnknownType() {
        new Type.Of("struct<a:string>").klass();
    }
    
    @Test
    public void hasToSting() {
        MatcherAssert.assertThat(
                new Type.Of("struct<a:string>"),
                Matchers.hasToString(
                        "Type.Of(dataType=StructType(StructField(a,StringType,true)))"
                )
        );
    }
    

    @Test
    public void parsesTimestamp() {
        MatcherAssert.assertThat(
                "timestamp is Timestamp",
                new Type.Of("timestamp").klass(),
                Matchers.is(Timestamp.class)
        );
    }
    
    @Test
    public void convertsDatatypeToJavaTypeForString() {
        MatcherAssert.assertThat(
                "string is String",
                new Type.Of("string").klass(),
                Matchers.is(String.class)
        );
    }

    @Test
    public void convertsDatatypeToJavaTypeForTimestamp() {
        MatcherAssert.assertThat(
                "timestamp is Timestamp",
                new Type.Of("timestamp").klass(),
                Matchers.is(Timestamp.class)
        );
    }
}