package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.util.function.Supplier;

interface Terminal {
    Path result();

    @RequiredArgsConstructor
    abstract class Envelope implements Terminal {
        private final Terminal orig;

        Envelope(final Supplier<Terminal> original) {
            this(original.get());
        }

        @Override
        public Path result() {
            return this.orig.result();
        }
    }
}
