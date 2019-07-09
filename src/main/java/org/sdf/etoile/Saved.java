package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.nio.file.Path;

@RequiredArgsConstructor
public final class Saved<X> implements Terminal {
    private final URI path;
    private final Output<X> tran;

    public Saved(final Path path, final Output<X> tran) {
        this(path.toUri(), tran);
    }

    @Override
    public URI result() {
        this.tran.get()
                .save(path.getPath());
        return path;
    }
}
