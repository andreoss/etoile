package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.function.Predicate;

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
