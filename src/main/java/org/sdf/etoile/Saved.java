package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.nio.file.Path;

@RequiredArgsConstructor
public final class Saved<X> implements Terminal {
    private final URI path;
    private final Output<X> tran;

    public Saved(final Path target, final Output<X> output) {
        this(target.toUri(), output);
    }

    @Override
    public URI result() {
        this.tran.get()
                .save(path.toString());
        return path;
    }
}
