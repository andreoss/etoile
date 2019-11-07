/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;

/**
 * Transformation of {@link Dataset}.
 *
 * @param <Y> Underlying data type.
 * @since 0.1.0
 */
interface Transformation<Y> {
    /**
     * Convert it back to dataset.
     *
     * @return The Spark dataset
     */
    Dataset<Y> get();

    /**
     * A @{link Transformation} which does nothing.
     *
     * @param <Y> Underlying type
     * @since 0.1.0
     */
    final class Noop<Y> extends Transformation.Envelope<Y> {
        /**
         * Ctor.
         *
         * @param tran A transformation
         */
        Noop(final Transformation<Y> tran) {
            super(() -> tran);
        }

        /**
         * Ctor.
         *
         * @param set A dataset
         */
        Noop(final Dataset<Y> set) {
            super(() -> set);
        }
    }

    /**
     * An Envelope for @{link Transformation}.
     *
     * @param <Y> Underlying data type
     * @since 0.1.0
     */
    @RequiredArgsConstructor
    abstract class Envelope<Y> implements Transformation<Y> {
        /**
         * A factory for Transformation.
         */
        private final Supplier<Transformation<Y>> factory;

        /**
         * Secondary ctor.
         *
         * @param set A dataset
         */
        Envelope(final Dataset<Y> set) {
            this(() -> set);
        }

        /**
         * Secondary ctor.
         *
         * @param trans A wrapped transformation.
         */
        Envelope(final Transformation<Y> trans) {
            this(() -> trans);
        }

        @Override
        public final Dataset<Y> get() {
            return this.factory.get()
                .get();
        }
    }
}
