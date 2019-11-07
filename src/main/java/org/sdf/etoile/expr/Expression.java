/*
 * Copyright(C) 2019
 */
package org.sdf.etoile.expr;

import org.apache.spark.sql.Column;

/**
 * An expression.
 *
 * @since 0.2.0
 */
@FunctionalInterface
public interface Expression {
    /**
     * Expression as column.
     *
     * @return A column.
     */
    Column get();
}
