/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.io.Serializable;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

/**
 * Supplier which throws {@link IllegalArgumentException } exception for non-serializable objects.
 *
 * @param <T> Object type
 * @since 0.2.0
 */
@RequiredArgsConstructor
public final class SerializableOnly<T> implements Supplier<T> {
    /**
     * Object which to check.
     */
    private final T object;

    @Override
    public T get() {
        if (this.object instanceof Serializable) {
            return this.object;
        }
        throw new IllegalArgumentException(
            String.format(
                "%s is not %s", this.object.getClass(), Serializable.class
            )
        );
    }
}
