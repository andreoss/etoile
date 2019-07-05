package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@RequiredArgsConstructor
final class StoredOutput<T> implements Terminal {
    private final Transformation<T> ds;
    private final Map<String, String> param;
    private final String codec;

    StoredOutput(
            final Transformation<T> input,
            final Map<String, String> parameters
    ) {
        this(
                new NumberedPartitions<>(
                        input,
                        partitions(parameters)
                ),
                parameters,
                parameters.getOrDefault("format", "csv")
        );
    }

    private static int partitions(final Map<String, String> param) {
        return Integer.parseUnsignedInt(param.getOrDefault("partitions", "1"));
    }

    @Override
    public Path result() {
        final Path result = Paths.get(param.get("path"));
        ds.get()
                .write()
                .format(codec)
                .options(param)
                .save(result.toAbsolutePath()
                        .toString());
        return result;
    }
}
