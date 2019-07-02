package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.nio.file.Path;

@RequiredArgsConstructor
public final class Saved<X> implements Terminal {
    private final Path path;
    private final Output<X> tran;

    @Override
    public Path result() {
        this.tran.get()
                .save(path.toAbsolutePath()
                        .toString());
        return path;
    }
}
