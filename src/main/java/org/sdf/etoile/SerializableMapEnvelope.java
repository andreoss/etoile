/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Map;
import org.apache.spark.api.java.JavaUtils;
import scala.collection.JavaConversions;

/**
 * Serializable envelope for {@link Map}.
 * @param <A> Key type.
 * @param <B> Value type.
 * @since 0.3.0
 */
public abstract class SerializableMapEnvelope<A, B>
    extends JavaUtils.SerializableMapWrapper<A, B> {
    /**
     * Ctor.
     * @param underlying Unrderlying map.
     */
    public SerializableMapEnvelope(final Map<A, B> underlying) {
        super(JavaConversions.mapAsScalaMap(underlying));
    }
}
