package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

interface Output<X> {
    DataFrameWriter<X> get();

    @RequiredArgsConstructor
    abstract class Envelope<X> implements Output<X> {
        @Delegate
        private final Output<X> delegate;

        public Envelope(final Supplier<Output<X>> delegate) {
            this(delegate.get());
        }
    }
}

@RequiredArgsConstructor
final class Mode<X> implements Output<X> {
    private final String mode;
    private final Output<X> orig;

    @Override
    public DataFrameWriter<X> get() {
        return this.orig.get()
                .mode(mode());
    }

    private SaveMode mode() {
        final Predicate<SaveMode> pred = x -> x.toString()
                .toLowerCase()
                .startsWith(mode.toLowerCase());
        return Arrays.stream(SaveMode.values())
                .filter(pred)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(mode));
    }
}

@RequiredArgsConstructor
public final class ParameterizedOutput<Y> implements Output<Y> {
    private final Transformation<Y> tran;
    private final Map<String, String> param;
    private final String format;

    @Override
    public DataFrameWriter<Y> get() {
        return this.tran.get()
                .write()
                .format(format)
                .options(param);
    }
}
