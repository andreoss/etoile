package org.sdf.etoile;

import org.apache.spark.api.java.JavaUtils;
import scala.collection.JavaConversions;

import java.util.Map;

abstract class SerializableMapEnvelop<A, B>
        extends JavaUtils.SerializableMapWrapper<A, B> {
    SerializableMapEnvelop(final Map<A, B> underlying) {
        super(JavaConversions.mapAsScalaMap(underlying));
    }
}
