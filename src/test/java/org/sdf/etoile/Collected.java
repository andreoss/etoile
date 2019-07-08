package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.List;

@RequiredArgsConstructor
final class Collected<X> implements List<X> {
    private final Transformation<X> tran;

    @Delegate
    private List<X> collected() {
        return this.tran.get()
                .collectAsList();
    }
}
