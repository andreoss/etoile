package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

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
        final Transformation<Row> raw = new Input(this.spark, inOpts);
        final Terminal terminal = new StoredOutput<>(
                new ColumnsDroppedByParameter<>(
                        new FullyCastedByParameters(
                                new SortedByParameter<>(
                                        new FullyCastedByParameters(
                                                raw,
                                                inOpts
                                        ), inOpts
                                ),
                                outOpts
                        ),
                        outOpts
                ),
                outOpts
        );
        terminal.run();
    }
}


