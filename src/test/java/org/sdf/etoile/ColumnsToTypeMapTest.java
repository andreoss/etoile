package org.sdf.etoile;

import org.cactoos.collection.CollectionOf;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapEntry;
import org.cactoos.map.MapOf;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

final class ColumnsToTypeMapTest {
    @Test
    void buildsMap() {
        MatcherAssert.assertThat(
            "creates a map",
            new ColumnsToTypeMap(
                new MapOf<>(
                    new MapEntry<>("string", new ListOf<>("name")),
                    new MapEntry<>("int", new ListOf<>("id", "id2"))
                ),
                new CollectionOf<>(
                    new MapOf<>(new MapEntry<>("int", "string")),
                    new MapOf<>(new MapEntry<>("timestamp", "string"))
                )
            ),
            Matchers.contains(
                Matchers.hasEntry("id", "string"),
                Matchers.hasEntry("id2", "string")
            )
        );
    }
}