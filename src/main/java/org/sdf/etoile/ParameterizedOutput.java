package org.sdf.etoile;

import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.util.Map;

@RequiredArgsConstructor
public final class ParameterizedOutput<Y> implements Terminal {
    private final Transformation<Y> tran;
    private final Map<String, String> param;
    private final String format;
    private final Path output;

    @Override
    public Path result() {
        this.tran.get()
                .write()
                .format(format)
                .options(param)
                .save(output.toAbsolutePath()
                        .toString());
        return output;
    }
}
