package org.sdf.etoile;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.util.Map;

@RequiredArgsConstructor
final class FullyCastedByParameters implements Transformation<Row> {
    private final Transformation<Row> first;
    private final Map<String, String> params;

    @Override
    public Dataset<Row> get() {
        final Transformation<Row> columnsCasted = new ColumnsCastedByParameters(
                first, "cast", params
        );
        return new ColumnsCastedToTypeMultiple(columnsCasted,
                new ColumnsToTypeMap(
                        new MappedKeysMap<>(
                                DataType::catalogString,
                                new TypeToColumnsMap(columnsCasted)
                        ),
                        new CastParameters("convert", params)
                )
        ).get();
    }
}
