/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.List;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ColumnsToTypeMap}.
 *
 * @since 0.3.5
 */
final class ColumnsToTypeMapTest {
    @Test
    void buildsMap() {
        final String strtype = "string";
        final String inttype = "int";
        final List<String> cols = new ListOf<>("id", "id2");
        MatcherAssert.assertThat(
            "creates a map",
            new ColumnsToTypeMap(
                new MapOf<>(
                    new MapEntry<>(strtype, new ListOf<>("name")),
                    new MapEntry<>(inttype, cols)
                ),
                new ListOf<>(
                    new MapOf<>(new MapEntry<>(inttype, strtype)),
                    new MapOf<>(new MapEntry<>("timestamp", inttype))
                )
            ),
            Matchers.contains(
                Matchers.hasEntry(cols.get(0), strtype),
                Matchers.hasEntry(cols.get(1), strtype)
            )
        );
    }
}
