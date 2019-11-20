/*
 * Copyright(C) 2019. See COPYING for more.
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
     * @param params Parameters.
     */
    SortedByParameter(final Transformation<Y> original,
        final Map<String, String> params) {
        super(
            () -> {
                final Transformation<Y> result;
                if (params.containsKey(SortedByParameter.PARAM_NAME)) {
                    final String prm = params.get(SortedByParameter.PARAM_NAME);
                    result = new Sorted<>(
                        original, prm.split(",")
                    );
                } else {
                    result = new Transformation.Noop<>(original);
                }
                return result;
            }
        );
    }
}
