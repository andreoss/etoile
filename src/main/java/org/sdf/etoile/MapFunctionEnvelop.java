package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import scala.Serializable;

@RequiredArgsConstructor
abstract class MapFunctionEnvelop<X, Y>
        implements MapFunction<X, Y>, Serializable {
    private final MapFunction<X, Y> map;

    public final Y call(X value) throws Exception {
        return this.map.call(value);
    }
}
