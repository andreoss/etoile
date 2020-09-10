/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;

/**
 * Output of tranformation.
 * @param <X> Underlying data type.
 * @since 0.2.0
 */
@FunctionalInterface
interface Output<X> {
    /**
     * Writer of encapsulated @{link Transformation}.
     * @return Writer.
     */
    DataFrameWriter<X> get();

    /**
     * Envelope for @{link Output}.
     * @param <X> Underlying data type.
     * @since 0.2.0
     */
    @RequiredArgsConstructor
    abstract class Envelope<X> implements Output<X> {
        /**
         * An Output to delegate to.
         */
        private final Output<X> delegate;

        /**
         * Ctor.
         * @param factory Factory object.
         */
        Envelope(final Supplier<Output<X>> factory) {
            this(factory.get());
        }

        @Override
        public final DataFrameWriter<X> get() {
            return this.delegate.get();
        }
    }
}

