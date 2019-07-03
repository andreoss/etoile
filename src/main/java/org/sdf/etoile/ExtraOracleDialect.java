package org.sdf.etoile;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.TimestampType$;
import scala.Option;
import scala.Some;

import java.sql.Types;

public final class ExtraOracleDialect extends JdbcDialect {
    public static final int DEFAULT_SCALE = 12;
    private static final int BINARY_FLOAT = 100;
    private static final int BINARY_DOUBLE = 101;
    private static final int TIMESTAMPTZ = -101;
    private static final long FLOAT_SCALE = -127L;

    @Override
    public boolean canHandle(final String url) {
        return url.startsWith("jdbc:oracle");
    }

    @Override
    public Option<DataType> getCatalystType(
            final int sqlType,
            final String typeName,
            final int size,
            final MetadataBuilder md
    ) {
        final Option<DataType> result;
        if (sqlType == Types.ROWID) {
            result = Some.apply(StringType$.MODULE$);
        } else if (
                sqlType == Types.TIMESTAMP_WITH_TIMEZONE
                        || sqlType == TIMESTAMPTZ
        ) {
            result = Some.apply(TimestampType$.MODULE$);
        } else if (sqlType == Types.NUMERIC) {
            final long scale;
            if (md != null) {
                scale = md.build()
                        .getLong("scale");
            } else {
                scale = DecimalType.MAX_SCALE();
            }
            if (scale == 0L) {
                result = decimalWithScale(DEFAULT_SCALE);
            } else if (scale == FLOAT_SCALE) {
                result = decimalWithScale(DecimalType.MAX_SCALE());
            } else {
                result = decimalWithScale((int) scale);
            }
        } else if (sqlType == BINARY_DOUBLE) {
            result = Some.apply(DoubleType$.MODULE$);
        } else if (sqlType == BINARY_FLOAT) {
            result = Some.apply(FloatType$.MODULE$);
        } else {
            result = super.getCatalystType(sqlType, typeName, size, md);
        }
        return result;
    }

    private Some<DataType> decimalWithScale(final int scale) {
        return Some.apply(
                DecimalType.apply(DecimalType.MAX_PRECISION(), scale)
        );
    }
}
