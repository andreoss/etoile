/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

/**
 * Row with simplified ctor.
 *
 * @since 0.7.0
 */
public final class TestRow extends GenericRowWithSchema {
    /**
     * Ctor.
     * @param ddl Schema.
     * @param elems Values.
     */
    public TestRow(final String ddl, final Object... elems) {
        super(elems, StructType.fromDDL(ddl));
    }
}

