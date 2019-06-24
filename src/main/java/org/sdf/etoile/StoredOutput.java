package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
final class StoredOutput<T> implements Terminal {
    private final Transformation<T> transformation;
    private final Map<String, String> param;
    private final String codec;

    StoredOutput(
            final Transformation<T> transformation,
            final Map<String, String> param
    ) {
        this(
                new NumberedPartitions<>(
                        transformation,
                        partitions(param)
                ),
                param,
                param.getOrDefault("format", "csv")
        );
    }

    private static int partitions(final Map<String, String> param) {
        return Integer.parseUnsignedInt(param.getOrDefault("partitions", "1"));
    }

    @Override
    public void run() {
        transformation.get()
                .write()
                .format(codec)
                .options(param)
                .save();
    }
}
