/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.cactoos.Func;
import org.cactoos.collection.Filtered;
import org.cactoos.collection.Mapped;
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
        this(directory, (File f) -> f.getName().endsWith(extension));
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
                            walk(directory)
                        )
                    )
                )
            )
        );
    }

    /**
     * Walk directory and collect files.
     * @param directory Directory.
     * @return Files.
     */
    private static Iterable<File> walk(final Path directory) {
        try {
            try (Stream<Path> files = Files.walk(directory)) {
                return files.map(Path::toFile)
                    .filter(File::isFile)
                    .collect(Collectors.toList());
            }
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
