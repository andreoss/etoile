/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.net.URI;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StringType$;

/**
 * Application.
 *
 * @todo Reduce coupling (1h)
 * @checkstyle ClassDataAbstractionCouplingCheck (100 lines)
 * @since 0.0.1
 */
@RequiredArgsConstructor
public final class Main implements Runnable {
    /**
     * A Spark session.
     */
    private final SparkSession spark;

    /**
     * Options for input.
     */
    private final Map<String, String> source;

    /**
     * Options for output.
     */
    private final Map<String, String> target;

    /**
     * Ctor.
     *
     * @param session A Spark session
     * @param args Command line arguments
     */
    public Main(final SparkSession session, final Map<String, String> args) {
        this(session,
            new PrefixArgs("input", args),
            new PrefixArgs("output", args)
        );
    }

    /**
     * Main method.
     *
     * @param args An array of command line arguments.
     */
    public static void main(final String... args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            JdbcDialects.registerDialect(new ExtraOracleDialect());
        } catch (final ClassNotFoundException ignored) {
        }
        new Main(
            SparkSession.builder()
                .getOrCreate(),
            new Args(args)
        ).run();
    }

    @Override
    public void run() {
        final String udfarg = "missing";
        this.spark.udf()
            .register(
                MissingUDF.MISSING_UDF_NAME, new MissingUDF(
                    this.source.getOrDefault(udfarg, "\u0001"),
                    this.target.getOrDefault(udfarg, "DEFAULT_VALUE")
                ), StringType$.MODULE$
            );
        final Transformation<Row> input = new Input(this.spark, this.source);
        final Transformation<Row> casted = new FullyCastedByParameters(
            input,
            this.source
        );
        final Transformation<Row> exprs = new ExpressionTransformed(
            casted,
            this.source
        );
        final Transformation<Row> sorted = new SortedByParameter<>(
            exprs,
            this.source
        );
        final Transformation<Row> recasted = new FullyCastedByParameters(
            sorted,
            this.target
        );
        final Transformation<Row> dropped = new ColumnsDroppedByParameter<>(
            recasted,
            this.target
        );
        final Transformation<Row> reparted = new NumberedPartitions<>(
            dropped,
            Integer.parseUnsignedInt(
                this.target.getOrDefault("partitions", "1")
            )
        );
        final Transformation<Row> replaced = this.replacedIfNeeded(reparted);
        final Output<Row> output = new FormatOutput<>(
            replaced,
            this.target
        );
        final Output<Row> mode = new Mode<>(
            this.target.getOrDefault(
                "mode",
                SaveMode.ErrorIfExists.name()
            ),
            output
        );
        final Terminal saved = new Saved<>(
            URI.create(this.target.get("path")),
            mode
        );
        saved.result();
    }

    /**
     * Replaced values in dataset if required by parameters.
     *
     * @param dataset A dataset
     * @return A dataset with values replaced
     */
    private Transformation<Row> replacedIfNeeded(
        final Transformation<Row> dataset) {
        final String replace = "replace";
        return new ConditionalTransformation<>(
            () -> this.target.containsKey(replace),
            new Substituted(
                new Stringified<>(dataset),
                new ReplacementMap(
                    this.target.get(replace)
                )
            ),
            dataset
        );
    }

}
