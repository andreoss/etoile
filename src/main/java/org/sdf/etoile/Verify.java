package org.sdf.etoile;

import java.io.Serializable;

public final class Verify {
    private Verify() {
    }

    public static <T> T isSerializable(final T obj) {
        return Verify.instanceOf(Serializable.class, obj);
    }

    private static <T> T instanceOf(final Class<?> klass, final T obj) {
        if (klass.isInstance(obj)) {
            return obj;
        }
        throw new IllegalArgumentException(
                String.format(
                        "%s is not %s", obj.getClass(), klass
                )
        );
    }
}
