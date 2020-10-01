/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

/**
 * Entry-point and command dispatcher.
 *
 * @since 0.6.0
 * @checkstyle ClassDataAbstractionCouplingCheck (100 lines)
 */
@RequiredArgsConstructor
public final class Main implements Runnable {
    /**
     * Command parameter name.
     */
    private static final String COMMAND = "command";

    /**
     * The Spark Session.
     */
    private final SparkSession spark;

    /**
     * Command-line arguments.
     */
    private final Map<String, String> args;

    /**
     * Main methods.
     * @param args Arguments.
     */
    @SuppressWarnings("squid:S4925")
    public static void main(final String... args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            JdbcDialects.registerDialect(new ExtraOracleDialect());
        } catch (final ClassNotFoundException ignored) {
        }
        new Main(
            SparkSession.builder().getOrCreate(),
            new Args(args)
        ).run();
    }

    @Override
    public void run() {
        final Runnable task;
        final String cmd = this.args.get(Main.COMMAND);
        if ("dump".equals(cmd) || cmd == null) {
            task = new Dump(this.spark, this.args);
        } else if ("pv".equals(cmd)) {
            task = new ValidatePartitions(this.spark, this.args);
        } else if ("compare".equals(cmd)) {
            task = new DiscrepancyMain(this.spark, this.args);
        } else {
            throw new IllegalArgumentException(
                String.format("unknown command: %s", this.args)
            );
        }
        task.run();
    }

}
