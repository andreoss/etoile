/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Rename columns for compatability with Hive.
 *
 * @since 0.4.0
 */
@RequiredArgsConstructor
public final class HiveCompatable implements Transformation<Row> {
    private static final Pattern PATTERN = Pattern.compile("[$#%]");

    /**
     * Original tranfomation.
     */
    private final Transformation<Row> original;

    @Override
    public Dataset<Row> get() {
        final Schema schema = new SchemaOf<>(this.original);
        final List<Alias> renames = new LinkedList<>();
        for (final String field : schema.fieldNames()) {
            final String renamed = HiveCompatable.PATTERN.matcher(field).replaceAll("_");
            if (!field.equals(renamed)) {
                renames.add(
                    new ColumnAlias(
                        field,
                        renamed.toUpperCase(Locale.getDefault())
                    )
                );
            }
        }
        return new Renamed(this.original, renames).get();
    }
}
