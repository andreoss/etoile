package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;

import java.util.function.Supplier;

interface Transformation<Y> extends Supplier<Dataset<Y>> {
    final class Noop<Y> extends Envelope<Y> {
        Noop(final Transformation<Y> df) {
            super(() -> df);
        }
        Noop(final Dataset<Y> ds) {
            super(() -> ds);
        }
    }

    @RequiredArgsConstructor
    abstract class Envelope<Y> implements Transformation<Y> {
        private final Supplier<Transformation<Y>> df;

        Envelope(final Dataset<Y> ds) {
            this(() -> ds);
        }

        Envelope(final Transformation<Y> trans) {
            this(() -> trans);
        }

        @Override
        public final Dataset<Y> get() {
            return this.df.get()
                    .get();
        }
    }
}
