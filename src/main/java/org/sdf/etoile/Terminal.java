/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.net.URI;

/**
 * Terminal operation.
 *
 * @since 0.1.0
 */
interface Terminal {
    /**
     * Returns locations of the result.
     *
     * @return A result location
     */
    URI result();
}
