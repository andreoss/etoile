/*
 * Copyright(C) 2019
 */
package org.sdf.etoile;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.types.DataType;

/**
 * Type.
 * @since 0.2.5
 */
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
@Generated
public final class TypeOf implements Type {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -303840972102172626L;

    /**
     * Spark type.
     */
    private final DataType type;

    /**
     * Ctor.
     * @param sql SQL type.
     */
    TypeOf(final String sql) {
        this(CatalystSqlParser$.MODULE$.parseDataType(sql));
    }

    @Override
    public DataType asSpark() {
        return this.type;
    }
}
