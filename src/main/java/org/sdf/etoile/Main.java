package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.nio.file.Paths;
import java.util.Map;


@RequiredArgsConstructor
public final class Main implements Runnable {
    private final SparkSession spark;
    private final Map<String, String> args;

    public static void main(final String[] args) {
        JdbcDialects.registerDialect(new ExtraOracleDialect());
        new Main(SparkSession.builder()
                .getOrCreate(), new Args(args)).run();
    }

    @Override
    public void run() {
        final Map<String, String> inOpts = new PrefixArgs("input", this.args);
        final Map<String, String> outOpts = new PrefixArgs("output", this.args);
        final Transformation<Row> input = new Input(this.spark, inOpts);
        new Saved<>(
                Paths.get(outOpts.get("path")),
                new Mode<>(
                        outOpts.getOrDefault("mode", SaveMode.ErrorIfExists.name()),
                        new StoredOutput<>(
                                new NumberedPartitions<>(
                                        new ColumnsDroppedByParameter<>(
                                                new FullyCastedByParameters(
                                                        new SortedByParameter<>(
                                                                new FullyCastedByParameters(
                                                                        input,
                                                                        inOpts
                                                                ), inOpts
                                                        ),
                                                        outOpts
                                                ),
                                                outOpts
                                        ),
                                        Integer.parseUnsignedInt(
                                                outOpts.getOrDefault("partitions", "1")
                                        )
                                ),
                                outOpts
                        )
                )
        ).result();
    }
}


