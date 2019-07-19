package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.spark.api.java.function.MapFunction;
import org.cactoos.Scalar;
import org.cactoos.scalar.UncheckedScalar;
import scala.Serializable;

@RequiredArgsConstructor
abstract class MapFunctionEnvelop<X, Y> implements MapFunction<X, Y>, Serializable {
    @Delegate
    private final MapFunction<X, Y> map;

    MapFunctionEnvelop(final Scalar<MapFunction<X, Y>> map) {
        this(new UncheckedScalar<>(map).value());
    }
}
