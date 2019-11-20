/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.Serializable;
import java.sql.Timestamp;
import org.apache.spark.sql.types.DataType;

/**
 * Type of column.
 * @since 0.2.5
 */
interface Type extends Serializable {

    /**
     * Convert to Java type.
     * @return Java type.
     */
    default Class<?> asJava() {
        final Class<?> result;
        if ("string".equals(this.asSql())) {
            result = String.class;
        } else if (this.asSql().startsWith("timestamp")) {
            result = Timestamp.class;
        } else {
            throw new UnsupportedOperationException();
        }
        return result;
    }

    /**
     * Convert to SQL type.
     * @return SQL type.
     */
    default String asSql() {
        return this.asSpark().catalogString();
    }

    /**
     * Convert to Spark type.
     * @return Spark type.
     */
    DataType asSpark();

}

