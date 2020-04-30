/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import java.io.Serializable;

/**
 * Comparison outcome.
 * Marked Serializable for Spark.
 *
 * @since 0.7.0
 */
public interface Outcome extends Serializable {
    /**
     * Indicates success.
     *
     * @return True if okay
     */
    boolean isOkay();

    /**
     * Description.
     *
     * @return Detailed result.
     */
    String description();

    /**
     * Indicates failure.
     *
     * @return True if something went wrong
     */
    default boolean isNotOkay() {
        return !this.isOkay();
    }
}
