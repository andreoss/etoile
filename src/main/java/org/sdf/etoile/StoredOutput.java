package org.sdf.etoile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

final class StoredOutput<T> extends Terminal.Envelope {

    StoredOutput(
            final Transformation<T> input,
            final Map<String, String> parameters
    ) {
        super(() -> {
                    final Path result = Paths.get(parameters.get("path"));
                    final String codec = parameters.getOrDefault(
                            "format", "csv"
                    );
                    final Transformation<T> repart = new NumberedPartitions<>(
                            input,
                            partitions(parameters)
                    );
                    if ("csv+header".equals(codec)) {
                        return new HeaderCsvOutput<>(
                                repart,
                                result,
                                parameters
                        );
                    } else {
                        return new ParametarizedOutput<>(
                                repart,
                                parameters,
                                codec,
                                result
                        );
                    }
                }
        );
    }

    private static int partitions(final Map<String, String> param) {
        return Integer.parseUnsignedInt(param.getOrDefault("partitions", "1"));
    }
}
