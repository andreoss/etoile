package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Row;

import java.nio.file.Path;

@RequiredArgsConstructor
public final class HeaderCsvOutput<T> implements Terminal {
    private final Transformation<T> tran;
    private final Path output;

    @Override
    public Path result() {
        final Transformation<Row> stringified = new Stringified<>(this.tran);
        new Union<>(
                new Header<>(stringified),
                stringified
        ).get()
                .coalesce(1)
                .write()
                .format("csv")
                .save(output.toAbsolutePath()
                        .toString());
        return output;
    }
}
