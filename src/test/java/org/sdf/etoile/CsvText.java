/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.cactoos.Text;
import org.cactoos.text.TextEnvelope;

/**
 * Csv files as {@link Text}.
 *
 * @since 0.2.5
 */
final class CsvText extends TextEnvelope {
    /**
     * Ctor.
     * @param input Terminal operation.
     */
    CsvText(final Terminal input) {
        this(input.result());
    }

    /**
     * Ctor.
     * @param uri URI of directory.
     */
    CsvText(final URI uri) {
        this(Paths.get(uri));
    }

    /**
     * Ctor.
     * @param input Directory.
     */
    private CsvText(final Path input) {
        super(
            new ConcatenatedText(input, "csv")
        );
    }

    /**
     * Ctor.
     * @param input Directory.
     */
    CsvText(final File input) {
        this(input.toPath());
    }
}
