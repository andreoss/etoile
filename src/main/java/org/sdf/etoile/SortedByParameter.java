/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Map;

/**
 * Sorted by parameters.
 * @param <Y> Underlying type.
 * @since 0.2.0
 */
final class SortedByParameter<Y> extends Transformation.Envelope<Y> {

    /**
     * Parameter name.
     */
    private static final String PARAM_NAME = "sort";

    /**
     * Ctor.
     * @param original Original transformation.
     * @param param Parameters.
     */
    SortedByParameter(final Transformation<Y> original,
        final Map<String, String> param) {
        super(
            () -> {
                final Transformation<Y> result;
                if (param.containsKey(SortedByParameter.PARAM_NAME)) {
                    result = new Sorted<>(
                        original, param.get(SortedByParameter.PARAM_NAME).split(",")
                    );
                } else {
                    result = new Transformation.Noop<>(original);
                }
                return result;
            }
        );
    }
}
