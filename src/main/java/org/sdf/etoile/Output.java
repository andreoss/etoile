package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.spark.sql.DataFrameWriter;

import java.util.function.Supplier;

interface Output<X> {
    DataFrameWriter<X> get();

    @RequiredArgsConstructor
    abstract class Envelope<X> implements Output<X> {
        @Delegate
        private final Output<X> delegate;

        Envelope(final Supplier<Output<X>> factory) {
            this(factory.get());
        }
    }
}
