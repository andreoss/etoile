package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public final class Main implements Runnable {
    private static final String DEFAULT_INPUT_FORMAT = "com.databricks.spark.avro";
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
        final Dataset<Row> result = sorted(input, orig);
        saveOutput(result);
    }

    private Dataset<Row> sorted(final Map<String, String> input, final Dataset<Row> orig) {
        final Dataset<Row> casted = fullCast(orig, input);
        final Dataset<Row> result;
        if (input.containsKey("sort")) {
            final Column expr = functions.expr(input.get("sort"));
            result = casted.sort(expr);
        } else {
            result = casted;

        }
        return result;
    }

    private Dataset<Row> fullCast(final Dataset<Row> orig, final Map<String, String> params) {
        final List<Map<String, String>> cast = parseCastParameter(params.getOrDefault("cast", ""));
        final Dataset<Row> columnsCasted = castTypes(orig, cast);
        final List<Map<String, String>> convert = parseCastParameter(params.getOrDefault("convert", ""));
        return castTypes(columnsCasted, remapTypesToColumns(columnsCasted, convert));
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
        final Dataset<Row> casted = fullCast(df, output);
        casted
                .coalesce(num)
                .write()
                .format(output.getOrDefault("format", DEFAULT_OUTPUT_FORMAT))
                .options(output)
                .save();
    }

    private List<Map<String, String>> remapTypesToColumns(final Dataset<Row> df, final Iterable<? extends Map<String, String>> types) {
        final Map<String, List<String>> cols = mapTypesToColumns(df);
        final List<Map<String, String>> result = new ArrayList<>();
        for (final Map<String, String> type : types) {
            for (final Map.Entry<String, String> entry : type.entrySet()) {
                final String from = entry.getKey();
                final String to = entry.getValue();
                if (cols.containsKey(from)) {
                    final List<String> xs = cols.get(from);
                    for (final String x : xs) {
                        result.add(
                                Collections.singletonMap(
                                        x, to
                                )
                        );
                    }
                }
            }
        }
        return result;
    }

    private Map<String, List<String>> mapTypesToColumns(final Dataset<Row> df) {
        final BinaryOperator<List<String>> merge = (a, b) -> Stream.of(a, b).flatMap(List::stream).collect(Collectors.toList());
        return Stream.of(df.schema().fields())
                .collect(
                        Collectors.toMap(
                                field -> field.dataType().catalogString().toLowerCase(),
                                field -> Collections.singletonList(field.name().toLowerCase()),
                                merge
                        )
                );
    }
}
