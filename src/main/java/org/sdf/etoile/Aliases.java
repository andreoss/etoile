/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.cactoos.Text;
import org.cactoos.collection.CollectionEnvelope;
import org.cactoos.iterable.Filtered;
import org.cactoos.iterable.Mapped;
import org.cactoos.list.ListOf;
import org.cactoos.text.IsBlank;
import org.cactoos.text.Split;

/**
 * Collection of aliases.
 *
 * @since 0.4.0
 */
final class Aliases extends CollectionEnvelope<Alias> {

    /**
     * Ctor.
     * @param expression Comma-separated expression.
     */
    Aliases(final String expression) {
        super(
            new ListOf<Alias>(
                new Mapped<String, Alias>(
                    ColumnAlias::new,
                    new Mapped<Text, String>(
                        Text::asString,
                        new Filtered<>(
                            x -> !new IsBlank(x).value(),
                            new Split(expression, ",")
                        )
                    )
                )
            )
        );
    }
}
