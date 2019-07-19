package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.cactoos.Scalar;
import org.cactoos.scalar.UncheckedScalar;
import scala.Serializable;

@RequiredArgsConstructor
abstract class MapFunctionEnvelop<X, Y> implements MapFunction<X, Y>, Serializable {
    private final MapFunction<X, Y> map;

    MapFunctionEnvelop(final Scalar<MapFunction<X, Y>> map) {
        this(new UncheckedScalar<>(map).value());
    }

    @Override
    public final Y call(final X value) throws Exception {
        return this.map.call(value);
    }
}
