package org.sdf.etoile;

import org.cactoos.list.ListEnvelope;

final class Collected<X> extends ListEnvelope<X> {
    Collected(final Transformation<X> tran) {
        super(tran.get()::collectAsList);
    }
}
