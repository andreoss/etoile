package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.spark.api.java.function.MapFunction;
import scala.Serializable;

@RequiredArgsConstructor
abstract class MapFunctionEnvelop<X, Y>
        implements MapFunction<X, Y>, Serializable {
    @Delegate
    private final MapFunction<X, Y> map;
}
