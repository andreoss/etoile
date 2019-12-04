/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link HiveCompatable}
 *
 * @since 0.4.0
 */
final class HiveCompatableTest extends SparkTestTemplate {
    @Test
    void changesSchemaToHiveCompatable() {
        MatcherAssert.assertThat(
            "removes special chars & and uppers case",
            new SchemaOf<>(
                new HiveCompatable(
                    new FakeInput(
                        this.session,
                        new StructType()
                            .add("i$d", StringType$.MODULE$)
                            .add("abx#xxx", StringType$.MODULE$),
                        Collections.emptyList()
                    )
                )
            ).fieldNames(),
            Matchers.contains(
                "I_D", "ABX_XXX"
            )
        );
    }
}