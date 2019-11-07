/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.sdf.etoile.util.MappedKeysMap;

/**
 * Casted by parameters.
 *
 * @since 0.2.0
 */
@RequiredArgsConstructor
final class FullyCastedByParameters implements Transformation<Row> {
    /**
     * Original tranformation.
     */
    private final Transformation<Row> first;

    /**
     * Casting parameters.
     */
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        final Transformation<Row> casted = new ColumnsCastedByParameters(
            this.first, "cast", this.params
        );
        return new ColumnsCastedToTypeMultiple(
            casted,
            new ColumnsToTypeMap(
                new MappedKeysMap<>(
                    DataType::catalogString,
                    new TypeToColumnsMap(casted)
                ),
                new Pairs("convert", this.params)
            )
        ).get();
    }
}
