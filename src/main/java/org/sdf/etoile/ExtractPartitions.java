/*
 * Copyright(C) 2019. See COPYING for more.
 */
package org.sdf.etoile;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Extract values of partition columns from input file name.
 *
 * @see org.apache.spark.sql.functions#input_file_name
 * @since 0.6.0
 */
final class ExtractPartitions implements Function<String, Map<String, String>> {

    @Override
    public Map<String, String> apply(final String filename) {
        final Map<String, String> res = new LinkedHashMap<>();
        final String[] parts = filename.split("/");
        for (int inx = parts.length - 2; inx >= 0; inx -= 1) {
            final String elem = parts[inx];
            if (!elem.contains("=")) {
                break;
            }
            final String[] kvs = elem.split("=", 2);
            res.put(kvs[0], kvs[1]);
        }
        return Collections.unmodifiableMap(res);
    }
}
