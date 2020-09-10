/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

/**
 * Detailed Outcome.
 *
 * @since 0.7.0
 */
public final class Detailed extends OutcomeEnvelope {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 6221137743275325723L;

    /**
     * Ctor.
     * @param desc Additional description.
     * @param orig Original outcome.
     */
    public Detailed(final String desc, final Outcome orig) {
        super(
            () -> {
                final Outcome result;
                final String description = String.format(
                    "%s: %s",
                    desc,
                    orig.description()
                );
                if (orig.isOkay()) {
                    result = new Okay(
                        description
                    );
                } else {
                    result = new Mismatch(
                        description
                    );
                }
                return result;
            }
        );
    }
}
