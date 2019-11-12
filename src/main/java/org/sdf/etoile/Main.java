package org.sdf.etoile;

import java.net.URI;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StringType$;


@RequiredArgsConstructor
public final class Main implements Runnable {
    private final SparkSession spark;
    private final Map<String, String> inOpts;
    private final Map<String, String> outOpts;

    public Main(final SparkSession session, final Map<String, String> args) {
        this(session,
            new PrefixArgs("input", args),
            new PrefixArgs("output", args)
        );
    }

    public static void main(final String[] args) {
        JdbcDialects.registerDialect(new ExtraOracleDialect());
        new Main(
            SparkSession.builder()
                .getOrCreate(),
            new Args(args)
        ).run();
    }

    @Override
    public void run() {
        this.spark.udf()
            .register(
                "missing", new MissingUDF(
                    this.inOpts.getOrDefault("missing", "\u0001"),
                    this.outOpts.getOrDefault("missing", "DEFAULT_VALUE")
                ), StringType$.MODULE$
            );
        final Transformation<Row> input = new Input(this.spark, inOpts);
        final Transformation<Row> casted = new FullyCastedByParameters(
            input,
            inOpts
        );
        final Transformation<Row> exprs = new ExpressionTransformed(
            casted,
            inOpts
        );
        final Transformation<Row> sorted = new SortedByParameter<>(
            exprs, inOpts
        );
        final Transformation<Row> castedAgain = new FullyCastedByParameters(
            sorted,
            outOpts
        );
        final Transformation<Row> dropped = new ColumnsDroppedByParameter<>(
            castedAgain,
            outOpts
        );
        final Transformation<Row> repartitioned = new NumberedPartitions<>(
            dropped,
            Integer.parseUnsignedInt(
                outOpts.getOrDefault("partitions", "1")
            )
        );
        final Transformation<Row> outReplaced =
            new ConditionalTransformation<>(
                () -> outOpts.containsKey("replace"),
                new Substituted(
                    new Stringified<>(repartitioned),
                    new ReplacementMap(
                        outOpts.get("replace")
                    )
                ),
                repartitioned
            );
        final Output<Row> output = new FormatOutput<>(
            outReplaced,
            outOpts
        );
        final Output<Row> mode = new Mode<>(
            outOpts.getOrDefault(
                "mode",
                SaveMode.ErrorIfExists.name()
            ),
            output
        );
        final Terminal saved = new Saved<>(
            URI.create(outOpts.get("path")),
            mode
        );
        saved.result();
    }
}


