/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;

/**
 * A {@link MapFunction} envelope.
 *
 * @param <X> Argument
 * @param <Y> Result
 * @since 0.2.0
 */
@RequiredArgsConstructor
abstract class MapFunctionEnvelop<X, Y> implements MapFunction<X, Y> {
    /**
     * The original function.
     */
    private final MapFunction<X, Y> map;

    @Override
    public final Y call(final X value) throws Exception {
        return this.map.call(value);
    }
}
