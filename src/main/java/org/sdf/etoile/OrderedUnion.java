package org.sdf.etoile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

final class OrderedUnion<Y> extends Transformation.Envelope<Row> {

    OrderedUnion(final Transformation<Y> left, final Transformation<Y> right) {
        this(left.get(), right.get(), "__order");
    }

    private OrderedUnion(
            final Dataset<Y> left,
            final Dataset<Y> right,
            final String pseudo
    ) {
        super(() -> {
                    final Dataset<Row> lord = left.withColumn(
                            pseudo,
                            functions.lit(0)
                    );
                    final Dataset<Row> rord = right.withColumn(
                            pseudo,
                            functions.monotonically_increasing_id()
                    );
                    return new NumberedPartitions<>(
                            new ColumnsDropped<>(
                                    new Sorted<>(
                                            new Union<>(lord, rord),
                                            pseudo
                                    ),
                                    pseudo
                            ),
                            1
                    );
                }
        );
    }
}
