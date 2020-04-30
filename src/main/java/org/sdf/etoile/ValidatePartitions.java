/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.nio.file.Paths;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.cactoos.list.Mapped;
import org.sdf.etoile.expr.ExpressionOf;

/**
 * Validate partitions runnable.
 * @since 0.7.0
 */
@RequiredArgsConstructor
final class ValidatePartitions implements Runnable {
    /**
     * The Spark Session.
     */
    private final SparkSession spark;

    /**
     * Command-line arguments.
     */
    private final Map<String, String> args;

    @Override
    public void run() {
        final Map<String, String> output = new PrefixArgs("output", this.args);
        new Saved<>(
            Paths.get(output.get("path")),
            new FormatOutput<>(
                new PartitionSchemeValidated(
                    new Input(this.spark, new PrefixArgs("input", this.args)),
                    new Mapped<>(
                        ExpressionOf::new,
                        new PrefixArgs(
                            "expression",
                            this.args
                        ).values()
                    )
                ),
                output
            )
        ).result();
    }
}
