/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Mismatch.
 *
 * @since 0.7.0
 */
@ToString
@RequiredArgsConstructor
@Generated
@EqualsAndHashCode
public final class Mismatch implements Outcome {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -4075439802268104124L;

    /**
     * Description.
     */
    private final String desc;

    @Override
    public boolean isOkay() {
        return false;
    }

    @Override
    public String description() {
        return this.desc;
    }
}
