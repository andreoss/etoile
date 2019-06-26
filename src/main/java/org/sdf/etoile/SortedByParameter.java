package org.sdf.etoile;

import java.util.Map;

final class SortedByParameter<Y> extends Transformation.Envelope<Y> {
    SortedByParameter(
            final Transformation<Y> tran,
            final Map<String, String> param
    ) {
        super(
                () -> {
                    if (param.containsKey("sort")) {
                        return new Sorted<>(
                                tran,
                                param.get("sort")
                                        .split(",")
                        );
                    } else {
                        return new Noop<>(tran);
                    }
                }
        );
    }
}
