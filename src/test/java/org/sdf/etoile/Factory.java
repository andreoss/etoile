/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

/**
 * Factories.
 * @since 0.3.2
 */
@SuppressWarnings("PMD.ProhibitPublicStaticMethods")
public final class Factory {
    /**
     * Utility class marker.
     */
    private Factory() {
    }

    /**
     * Create an array.
     * @param values Values.
     * @param <K> Type of values.
     * @return An array.
     */
    @SafeVarargs
    public static <K> K[] arrayOf(final K... values) {
        return values;
    }
}
