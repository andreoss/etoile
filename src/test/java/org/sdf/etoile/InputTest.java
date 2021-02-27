/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Input}.
 *
 * @since 0.7.0
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class InputTest extends SparkTestTemplate {

    @Test
    void handlesNotExistentTable() {
        this.data.writeInput(
            "0,0,XXX",
            "1,0,wtf"
        );
        this.session().udf().register(
            "missing",
            new MissingUDF("XXX", "YYY"),
            DataTypes.StringType
        );
        this.session().sql("CREATE DATABASE IF NOT EXISTS FOO");
        this.session().sql("DROP TABLE IF EXISTS FOO.BARX");
        final Transformation<Row> input = new Input(
            this.session(),
            new MapOf<>(
                new MapEntry<>("table", "FOO.BARX")
            )
        );
        MatcherAssert.assertThat(
            Assertions.assertThrows(AnalysisException.class, input::get),
            Matchers.hasToString(
                Matchers.containsString("Table or view not found")
            )
        );
    }

    @Test
    void handlesMissing() {
        this.data.writeInput(
            "0,0,XXX",
            "1,0,wtf"
        );
        this.session().udf().register(
            "missing",
            new MissingUDF("XXX", "YYY"),
            DataTypes.StringType
        );
        MatcherAssert.assertThat(
            new Input(
                this.session(),
                new MapOf<>(
                    new MapEntry<>("path", this.data.input().toString()),
                    new MapEntry<>("format", "csv+missing")
                )
            ),
            new HasRows<>(
                "[0,0,YYY]",
                "[1,0,wtf]"
            )
        );
    }

    @Test
    void readsInputFromPath() {
        this.data.writeInput(
            "id,ctl_validfrom,name",
            "0,0,abc",
            "1,0,xyz"
        );
        MatcherAssert.assertThat(
            new Input(
                this.session(),
                new MapOf<>(
                    new MapEntry<>("path", this.data.input().toString()),
                    new MapEntry<>("format", "csv")
                )
            ),
            Matchers.not(new IsEmpty<>())
        );
    }

}
