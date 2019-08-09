package org.sdf.etoile;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public class ConditionalTransformationTest extends SparkTestTemplate {

    @Test
    public void returnsLeftIfConditionIsFalse() {
        MatcherAssert.assertThat(
                "uses left transformation if false",
                new SchemaOf<>(
                        new ConditionalTransformation<>(
                                () -> false,
                                new Transformation.Noop<>(new FakeInput(
                                        session, "id int"
                                )),
                                new Transformation.Noop<>(new FakeInput(
                                        session, "id string"
                                ))
                        )
                ).asMap(),
                Matchers.hasEntry("id", "string")
        );
    }

    @Test
    public void returnsRightIfConditionMeets() {
        MatcherAssert.assertThat(
                "uses right transformation if true",
                new SchemaOf<>(
                        new ConditionalTransformation<>(
                                () -> true,
                                new Transformation.Noop<>(new FakeInput(
                                        session, "id int"
                                )),
                                new Transformation.Noop<>(new FakeInput(
                                        session, "id string"
                                ))
                        )
                ).asMap(),
                Matchers.hasEntry("id", "int")
        );
    }
}
