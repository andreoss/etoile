/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Arrays;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.cactoos.collection.Mapped;

/**
 * Renamed according to aliases.
 *
 * @since 0.4.0
 */
@RequiredArgsConstructor
public final class Renamed implements Transformation<Row> {
    /**
     * The original tranformation.
     */
    private final Transformation<Row> original;

    /**
     * Column aliases.
     */
    private final Collection<Alias> aliases;

    /**
     * Ctor.
     * @param aliases Alias.
     * @param original The original transformation.
     */
    public Renamed(final Collection<String> aliases, final Transformation<Row> original) {
        this(original, new Mapped<>(ColumnAlias::new, aliases));
    }

    /**
     * Ctor.
     * @param original The original.
     * @param aliases Aliases as strings.
     */
    public Renamed(final Transformation<Row> original, final String... aliases) {
        this(Arrays.asList(aliases), original);
    }

    @Override
    public Dataset<Row> get() {
        Dataset<Row> memo = this.original.get();
        for (final Alias alias : this.aliases) {
            memo = memo.withColumnRenamed(alias.reference(), alias.alias());
        }
        return memo;
    }
}
