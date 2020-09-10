/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

/**
 * Tranformation with constant number of parititons.
 *
 * Can be used in order to produce limited number of output files.
 * @param <T> Underlying data type.
 *
 * @since 0.2.1
 */
final class NumberedPartitions<T> extends TransformationEnvelope<T> {
    /**
     * Ctor.
     * @param trans Original tranformation.
     * @param num Number of partitions.
     */
    NumberedPartitions(final Transformation<T> trans, final int num) {
        super(trans.get().coalesce(num));
    }
}
