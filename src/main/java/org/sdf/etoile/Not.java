/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.Serializable;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;

/**
 * Negate filter.
 * @param <X> Underlying datatype.
 *
 * @since 0.6.0
 */
@RequiredArgsConstructor
final class Not<X> implements FilterFunction<X>, Serializable {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 2411676849539545831L;

    /**
     * Original filter.
     */
    private final FilterFunction<X> filter;

    @Override
    public boolean call(final X value) throws Exception {
        return !this.filter.call(value);
    }
}
