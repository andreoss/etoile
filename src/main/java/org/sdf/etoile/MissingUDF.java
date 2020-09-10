/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile;

import java.util.regex.Pattern;
import org.apache.spark.sql.api.java.UDF1;

/**
 * UDF for replacing missing flag with string literal.
 *
 * @since 0.3.2
 * @checkstyle AbbreviationAsWordInNameCheck (3 lines)
 */
public final class MissingUDF implements UDF1<Object, String> {
    /**
     * Default name of UDF.
     */
    public static final String MISSING_UDF_NAME = "missing";

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 5416439547387768779L;

    /**
     * Pattern to replace.
     */
    private final String pattern;

    /**
     * Replecement literal.
     */
    private final String replecement;

    /**
     * Default ctor.
     */
    public MissingUDF() {
        this("\u0001", "DEFAULT_VALUE");
    }

    /**
     * Ctor.
     * @param pttrn Pattern.
     * @param rplcmnt Replacement.
     */
    public MissingUDF(final String pttrn, final String rplcmnt) {
        this.pattern = pttrn;
        this.replecement = rplcmnt;
    }

    @Override
    public String call(final Object value) {
        final String result;
        if (this.isMissing(value)) {
            result = this.replecement;
        } else {
            result = String.valueOf(value);
        }
        return result;
    }

    /**
     * Check if value is `missing`.
     *
     * @param value Original value.
     * @return True if matches pattern.
     */
    private boolean isMissing(final Object value) {
        return this.pattern.equals(MissingUDF.trimmed(value));
    }

    /**
     * Trim spaces.
     *
     * @param value Original.
     * @return Trimmed string.
     */
    private static String trimmed(final Object value) {
        return Pattern.compile("^\\s+|\\s+$")
            .matcher(String.valueOf(value))
            .replaceAll("");
    }
}
