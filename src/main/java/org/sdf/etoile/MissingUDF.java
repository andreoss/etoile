package org.sdf.etoile;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Pattern;

public final class MissingUDF implements UDF1<Object, String> {
    private static final String MISSING = "\u0001";
    private static final Pattern TRIMMED = Pattern.compile("^\\s+|\\s+$");

    @Override
    public String call(final Object value) {
        final String result;
        if (isMissing(value)) {
            result = "MISSING";
        } else {
            result = String.valueOf(value);
        }
        return result;
    }

    private boolean isMissing(final Object value) {
        return MISSING.equals(trimmed(value));
    }

    private String trimmed(final Object value) {
        return TRIMMED.matcher(String.valueOf(value)).replaceAll("");
    }
}
