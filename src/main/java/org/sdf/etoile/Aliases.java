/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import org.cactoos.Text;
import org.cactoos.collection.CollectionEnvelope;
import org.cactoos.collection.Filtered;
import org.cactoos.list.Mapped;
import org.cactoos.scalar.Not;
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
            () ->
                new Mapped<>(
                    ColumnAlias::new,
                    new Mapped<>(
                        Text::asString,
                        new Filtered<>(
                            x -> new Not(new IsBlank(x)).value(),
                            new Split(expression, ",")
                        )
                    )
                )
        );
    }
}
