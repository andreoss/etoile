/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.nio.file.Paths;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.cactoos.list.Mapped;
import org.sdf.etoile.expr.ExpressionOf;

/**
 * Entiry point and command dispatcher.
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
    public static void main(final String... args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (final ClassNotFoundException ignored) {
        }
        JdbcDialects.registerDialect(new ExtraOracleDialect());
        new Main(
            SparkSession.builder().getOrCreate(),
            new Args(args)
        ).run();
    }

    @Override
    public void run() {
        final Runnable task;
        if ("dump".equals(this.args.get(Main.COMMAND))) {
            task = new Dump(this.spark, this.args);
        } else if ("pv".equals(this.args.get(Main.COMMAND))) {
            task = () -> {
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
            };
        } else {
            throw new IllegalArgumentException(
                String.format("command is not set: %s", this.args)
            );
        }
        task.run();
    }
}
