package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        final Dataset<Row> orig = this.spark.read()
                .format(input.getOrDefault("format", DEFAULT_INPUT_FORMAT))
                .options(input)
                .load();

        final Dataset<Row> dfWithCasts = castTypes(orig, parseCastParameter(input.getOrDefault("cast", "")));
        final Dataset<Row> result;
        if (input.containsKey("sort")) {
            final Column expr = functions.expr(input.get("sort"));
            result = dfWithCasts.sort(expr);
        } else {
            result = dfWithCasts;

        }
        saveOutput(result);
    }

    private List<Map<String, String>> parseCastParameter(final String cast) {
        final String colSep = ",";
        final String typeSep = ":";
        return Stream.of(cast.split(colSep))
                .filter(s -> !s.isEmpty())
                .map(s -> Arrays.asList(s.split(typeSep)))
                .map(l -> Collections.singletonMap(l.get(0), l.get(1)))
                .collect(Collectors.toList());
    }

    private Dataset<Row> castTypes(final Dataset<Row> orig, final List<Map<String, String>> casts) {
        Dataset<Row> copy = orig;
        for (final Map<String, String> map : casts) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                final String name = entry.getKey();
                final String type = entry.getValue();
                final Column cast = copy.col(name).cast(type);
                copy = copy.withColumn(name, cast);
            }
        }
        return copy;
    }

    private void saveOutput(final Dataset<Row> df) {
        final Map<String, String> output = new PrefixArgs("output", this.args);

        final int num = Integer.parseUnsignedInt(output.getOrDefault("partitions", "1"));
        castTypes(df, parseCastParameter(output.getOrDefault("cast", "")))
                .coalesce(num)
                .write()
                .format(output.getOrDefault("format", DEFAULT_OUTPUT_FORMAT))
                .options(output)
                .save();
    }
}
