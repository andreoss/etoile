package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;

import java.util.Map;

@RequiredArgsConstructor
public final class Main implements Runnable {
    public static final String DEFAULT_INPUT_FORMAT = "com.databricks.spark.avro";
    private static final String DEFAULT_OUTPUT_FORMAT = "csv";
    private final SparkSession spark;
    private final Map<String, String> args;

    public static void main(final String[] args) {
        new Main(
                SparkSession.builder()
                        .getOrCreate(),
                new Args(args)
        ).run();
    }

    @Override
    public void run() {
        final Map<String, String> input = new PrefixArgs("input", this.args);
        final Dataset<Row> df = this.spark.read()
                .format(input.getOrDefault("format", DEFAULT_INPUT_FORMAT))
                .options(input)
                .load();
        if (input.containsKey("sort")) {
            final Column expr = functions.expr(input.get("sort"));
            saveOutput(
                    df.sort(expr)
            );
        }
        else {
            saveOutput(df);
        }
    }

    private void saveOutput(final Dataset<Row> df) {
        final Map<String, String> output = new PrefixArgs("output", this.args);
        df.write()
                .format(output.getOrDefault(
                        "format", DEFAULT_OUTPUT_FORMAT
                        )
                )
                .options(output)
                .save();
    }
}
