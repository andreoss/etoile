package org.sdf.etoile;

final class NumberedPartitions<T> extends Transformation.Envelope<T> {
    NumberedPartitions(final Transformation<T> df, final int num) {
        super(df.get()
                .coalesce(num));
    }
}
