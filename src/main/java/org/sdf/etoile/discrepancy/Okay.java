/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Okay.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
@ToString
@Generated
@EqualsAndHashCode
public final class Okay implements Outcome {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 4122941626209464644L;

    /**
     * A description.
     */
    private final String desc;

    /**
     * Ctor.
     */
    public Okay() {
        this("OK");
    }

    @Override
    public boolean isOkay() {
        return true;
    }

    @Override
    public String description() {
        return this.desc;
    }
}
