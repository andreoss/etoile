/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;

/**
 * Sorted by parameters.
 * @param <Y> Underlying type.
 * @since 0.2.0
 */
final class SortedByParameter<Y> extends TransformationEnvelope<Y> {

    /**
     * Parameter name.
     */
    private static final String PARAM_NAME = "sort";

    /**
     * Ctor.
     * @param original Original transformation.
     * @param params Parameters.
     */
    SortedByParameter(
        final Transformation<Y> original,
        final Map<String, String> params
    ) {
        super(
            new ConditionalTransformation<>(
                () -> params.containsKey(SortedByParameter.PARAM_NAME),
                () -> new Sorted<>(
                    original,
                    params.get(SortedByParameter.PARAM_NAME).split(",")
                ).get(),
                new Noop<>(original)
            )
        );
    }
}
