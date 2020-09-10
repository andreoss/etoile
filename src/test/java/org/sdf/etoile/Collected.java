/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.cactoos.list.ListEnvelope;

/**
 * A transformation collected to List.
 *
 * @param <X> Underlying data type.
 * @since 0.2.0
 */
final class Collected<X> extends ListEnvelope<X> {
    /**
     * Ctor.
     *
     * @param tran A tranformation to collect.
     */
    Collected(final Transformation<X> tran) {
        super(tran.get()::collectAsList);
    }
}
