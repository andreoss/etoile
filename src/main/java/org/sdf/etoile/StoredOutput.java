package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
final class StoredOutput<T> implements Terminal {
    private final Transformation<T> transformation;
    private final String codec;
    private final Map<String, String> param;

    StoredOutput(
            final Transformation<T> transformation,
            final Map<String, String> param
    ) {
        this(
                new NumberedPartitions<>(
                        transformation,
                        partitions(param)
                ),
                param.getOrDefault("format", "csv"), param);
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
