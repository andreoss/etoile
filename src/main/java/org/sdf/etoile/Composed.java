/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import org.apache.spark.api.java.function.MapFunction;

/**
 * A composition of two @{link MapFunction}s.
 *
 * @param <A> Argument type.
 * @param <B> Linking type.
 * @param <C> Result type.
 * @since 0.2.3
 */
final class Composed<A, B, C> extends MapFunctionEnvelop<A, C> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -303840972102172626L;

    /**
     * Ctor.
     *
     * @param first First function to apply.
     * @param second Second function to apply.
     */
    Composed(final MapFunction<A, B> first, final MapFunction<B, C> second) {
        super(a -> second.call(first.call(a)));
    }
}
