/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

/**
 * Outcome Envelope.
 * Preserves equality.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
public abstract class OutcomeEnvelope implements Outcome {
    /**
     * The delegate.
     */
    private final Outcome original;

    /**
     * Secondary ctor.
     * @param factory Factory for Outcome.
     */
    protected OutcomeEnvelope(final Supplier<Outcome> factory) {
        this(factory.get());
    }

    @Override
    public final boolean isOkay() {
        return this.original.isOkay();
    }

    @Override
    public final String description() {
        return this.original.description();
    }

    @Override
    public final int hashCode() {
        return this.original.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        return this.original.equals(obj);
    }

    @Override
    public final String toString() {
        return this.original.toString();
    }
}
