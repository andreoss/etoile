package org.sdf.etoile.expr;

import org.apache.spark.sql.Column;

import java.util.function.Supplier;

public interface Expression extends Supplier<Column> {
}
