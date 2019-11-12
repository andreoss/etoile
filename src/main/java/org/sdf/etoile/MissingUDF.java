package org.sdf.etoile;

import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.api.java.UDF1;

@RequiredArgsConstructor
public final class MissingUDF implements UDF1<Object, String> {
    private static final Pattern TRIMMED = Pattern.compile("^\\s+|\\s+$");
    private final String token;
    private final String replacement;

    public MissingUDF() {
        this("\u0001", "DEFAULT_VALUE");
    }

    @Override
    public String call(final Object value) {
        final String result;
        if (isMissing(value)) {
            result = replacement;
        } else {
            result = String.valueOf(value);
        }
        return result;
    }

    private boolean isMissing(final Object value) {
        return token.equals(trimmed(value));
    }

    private String trimmed(final Object value) {
        return TRIMMED.matcher(String.valueOf(value))
            .replaceAll("");
    }
}
