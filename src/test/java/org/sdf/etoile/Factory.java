package org.sdf.etoile;

public final class Factory {
    @SafeVarargs
    public static <K> K[] arrayOf(final K... xs) {
        return xs;
    }
}
