package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.net.URI;
import java.util.Map;


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
                SparkSession.builder().getOrCreate(),
                new Args(args)
        ).run();
    }

    @Override
    public void run() {
        final Transformation<Row> input = new Input(this.spark, inOpts);
        final Transformation<Row> casted = new FullyCastedByParameters(
                input,
                inOpts
        );
        final Transformation<Row> sorted = new SortedByParameter<>(
                casted, inOpts
        );
        final Transformation<Row> castedAgain = new FullyCastedByParameters(
                sorted,
                outOpts
        );
        final Transformation<Row> expressed = new Transformed(
                sorted,
                inOpts
        );
        final Transformation<Row> dropped = new ColumnsDroppedByParameter<>(
                expressed,
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


