/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.sql.Types;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.TimestampType$;
import scala.Option;
import scala.Some;

/**
 * Work/around for currenly unhandled by Spark types.
 * @since 0.3.0
 * @todo Test cover.
 */
public final class ExtraOracleDialect extends JdbcDialect {
    /**
     * Default scale for binary types.
     */
    private static final int DEFAULT_SCALE = 10;

    /**
     * JDBC id for `binary_float`.
     */
    private static final int BINARY_FLOAT = 100;

    /**
     * JDBC id for `binary_double`.
     */
    private static final int BINARY_DOUBLE = 101;

    /**
     * JDBC id for `timestamp with timezone`.
     */
    private static final int TIMESTAMPTZ = -101;

    /**
     * Default scale for `float`.
     */
    private static final long FLOAT_SCALE = -127L;

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 6435496248658861530L;

    @Override
    public boolean canHandle(final String url) {
        return url.startsWith("jdbc:oracle");
    }

    @Override
    //@checkstyle ParameterNumberCheck (2 lines)
    public Option<DataType> getCatalystType(final int sql, final String name,
        final int size, final MetadataBuilder metabuilder) {
        final Option<DataType> result;
        if (sql == Types.ROWID) {
            result = Some.apply(DataTypes.StringType);
        } else if (
            sql == Types.TIMESTAMP_WITH_TIMEZONE
                || sql == ExtraOracleDialect.TIMESTAMPTZ
        ) {
            result = Some.apply(TimestampType$.MODULE$);
        } else if (sql == Types.NUMERIC) {
            result = ExtraOracleDialect.handleNumeric(metabuilder);
        } else if (sql == ExtraOracleDialect.BINARY_DOUBLE) {
            result = Some.apply(DataTypes.DoubleType);
        } else if (sql == ExtraOracleDialect.BINARY_FLOAT) {
            result = Some.apply(DataTypes.FloatType);
        } else {
            result = super.getCatalystType(sql, name, size, metabuilder);
        }
        return result;
    }

    /**
     * Handle numeric types.
     * @param metabuilder Metadata of column
     * @return Spark data type.
     */
    private static Option<DataType> handleNumeric(
        final MetadataBuilder metabuilder) {
        final long scale;
        if (metabuilder == null) {
            scale = 0L;
        } else {
            scale = metabuilder.build().getLong("scale");
        }
        final Option<DataType> result;
        if (scale == 0L) {
            result = Some.apply(ExtraOracleDialect.decimalWithScale((int) 0L));
        } else if (scale == ExtraOracleDialect.FLOAT_SCALE) {
            result = Some.apply(
                ExtraOracleDialect.decimalWithScale(
                    ExtraOracleDialect.DEFAULT_SCALE
                )
            );
        } else {
            result = Option.empty();
        }
        return result;
    }

    /**
     * Decimal type with maximal precision and certain scale.
     * @param scale Scale
     * @return Spark data type.
     */
    private static DataType decimalWithScale(final int scale) {
        return DecimalType.apply(DecimalType.MAX_PRECISION(), scale);
    }
}
