/*
 * Copyright(C) 2019, 2020. See LICENSE for more.
 */
package org.sdf.etoile.discrepancy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.cactoos.list.ListOf;

/**
 * Several {@link Outcome}s grouped toghether.
 *
 * @since 0.7.0
 */
public final class GroupOutcome extends OutcomeEnvelope {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -7746651774303109371L;

    /**
     * Secondary vararg ctor.
     * @param outcomes Outcomes.
     */
    public GroupOutcome(final Outcome... outcomes) {
        this(new ListOf<>(outcomes));
    }

    /**
     * Ctor.
     * @param outcomes Outcomes.
     */
    public GroupOutcome(final Collection<Outcome> outcomes) {
        super(
            () -> {
                final Outcome result;
                final Map<Boolean, List<Outcome>> grouped =
                    outcomes
                        .stream()
                        .collect(Collectors.groupingBy(Outcome::isOkay));
                if (grouped.containsKey(false)) {
                    result = new Mismatch(
                        String.format(
                            "Failure(%d/%d): %s",
                            grouped.get(false).size(),
                            outcomes.size(),
                            grouped.get(false).stream()
                                .map(Outcome::description)
                                .collect(Collectors.joining(", "))
                        )
                    );
                } else {
                    result = new Okay(
                        String.format("OK(%d)", outcomes.size())
                    );
                }
                return result;
            });
    }
}
