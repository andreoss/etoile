package org.sdf.etoile;

import org.apache.spark.api.java.function.MapFunction;

final class ComposedMapFunction<A, B, C> extends MapFunctionEnvelop<A, C> {
    ComposedMapFunction(
            final MapFunction<A, B> first,
            final MapFunction<B, C> second
    ) {
        super(a -> second.call(first.call(a)));
    }
}
