package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;

import java.util.function.Supplier;

interface Output<X> {
    DataFrameWriter<X> get();

    @RequiredArgsConstructor
    abstract class Envelope<X> implements Output<X> {
        private final Output<X> delegate;

        Envelope(final Supplier<Output<X>> factory) {
            this(factory.get());
        }

        public final DataFrameWriter<X> get() {
            return this.delegate.get();
        }
    }
}
