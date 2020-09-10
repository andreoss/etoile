/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import lombok.RequiredArgsConstructor;

/**
 * Saves output to disk.
 *
 * @param <X> Output underlying data type.
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class Saved<X> implements Terminal {
    /**
     * Location on disk/fs.
     */
    private final URI path;

    /**
     * Output to write.
     */
    private final Output<X> tran;

    /**
     * Secondary ctor.
     * @param file Target file.
     * @param output Output.
     */
    public Saved(final File file, final Output<X> output) {
        this(file.toPath(), output);
    }

    /**
     * Secondary ctor.
     * @param target Target path.
     * @param output Output.
     */
    public Saved(final Path target, final Output<X> output) {
        this(target.toUri(), output);
    }

    @Override
    public URI result() {
        this.tran.get().save(this.path.toString());
        return this.path;
    }
}
