package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.util.function.Supplier;

interface Terminal {
    URI result();

    @RequiredArgsConstructor
    abstract class Envelope implements Terminal {
        private final Terminal orig;

        Envelope(final Supplier<Terminal> original) {
            this(original.get());
        }

        @Override
        public URI result() {
            return this.orig.result();
        }
    }
}
