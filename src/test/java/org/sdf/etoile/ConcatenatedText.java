/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.nio.file.Path;
import org.cactoos.Func;
import org.cactoos.collection.Filtered;
import org.cactoos.collection.Mapped;
import org.cactoos.iterable.IterableOf;
import org.cactoos.iterable.NoNulls;
import org.cactoos.text.Joined;
import org.cactoos.text.TextEnvelope;
import org.cactoos.text.TextOf;

/**
 * Concatenate files to a single {@link Text} object.
 *
 * @since 0.3.0
 */
final class ConcatenatedText extends TextEnvelope {
    /**
     * Ctor.
     * @param directory Directory with files.
     * @param extension Extention.
     */
    ConcatenatedText(final Path directory, final String extension) {
        this(directory, (File f) -> f.getName()
            .endsWith(extension));
    }

    /**
     * Ctor.
     * @param directory Directory with files.
     * @param filter File filter.
     */
    private ConcatenatedText(final Path directory, final Func<File, Boolean> filter) {
        super(
            new Joined(
                new TextOf(""),
                new Mapped<>(
                    TextOf::new,
                    new Filtered<>(
                        filter,
                        new NoNulls<>(
                            new IterableOf<>(
                                directory.toFile().listFiles(File::isFile)
                            )
                        )
                    )
                )
            )
        );
    }
}
