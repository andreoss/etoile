/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.sdf.etoile.discrepancy.EqualsComparison;

/**
 * Discrepancy runnable.
 * @since 0.7.0
 */
@RequiredArgsConstructor
final class DiscrepancyMain implements Runnable {
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
                new Discrepancy(
                    Arrays.asList(this.args.get("keys").split("\\s*,\\s*")),
                    new Input(this.spark, new PrefixArgs("right", this.args)),
                    new Input(this.spark, new PrefixArgs("left", this.args)),
                    new Compare(new EqualsComparison())
                ),
                output
            )
        ).result();
    }
}
