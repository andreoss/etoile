package org.sdf.etoile;

import java.io.IOException;
import org.cactoos.list.ListOf;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

final class UnionTest extends SparkTestTemplate {

    @Test
    void unionOfTwo() throws IOException {
        MatcherAssert.assertThat(
            "union of two datasets",
            new CsvText(
                new Saved<>(
                    this.temp.newFolder().toPath().resolve("out"),
                    new HeaderCsvOutput<>(
                        new Union<>(
                            new FakeInput(this.session, "id int, name string",
                                new ListOf<>(
                                    new Object[]{1, "foo"},
                                    new Object[]{2, "bar"}
                                )
                            ),
                            new FakeInput(this.session, "id int, name string",
                                new ListOf<>(
                                    new Object[]{3, "baz"},
                                    new Object[]{4, "fee"}
                                )
                            )
                        )
                    )
                )
            ),
            new LinesAre(
                "id,name",
                "1,foo",
                "2,bar",
                "3,baz",
                "4,fee"
            )
        );
    }
}