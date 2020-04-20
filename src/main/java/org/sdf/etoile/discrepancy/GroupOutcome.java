/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.cactoos.list.ListOf;

/**
 * Several {@link Outcome}s grouped toghether.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
@ToString
@EqualsAndHashCode
public final class GroupOutcome implements Outcome {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -7746651774303109371L;

    /**
     * The group.
     */
    private final Collection<Outcome> outcomes;

    /**
     * Secondary vararg ctor.
     * @param otcms Outcomes.
     */
    public GroupOutcome(final Outcome... otcms) {
        this(new ListOf<>(otcms));
    }

    @Override
    public boolean isOkay() {
        return this.outcomes.stream().allMatch(Outcome::isOkay);
    }

    @Override
    public String description() {
        final String result;
        final Map<Boolean, List<Outcome>> grouped =
            this.outcomes
                .stream()
                .collect(Collectors.groupingBy(Outcome::isOkay));
        if (grouped.get(false).isEmpty()) {
            result = String.format("OK(%s)", grouped.size());
        } else {
            result = String.format(
                "Failure(%d/%d): %s",
                grouped.get(true).size(),
                this.outcomes.size(),
                grouped.get(false).stream()
                    .map(Outcome::description)
                    .collect(Collectors.joining(", "))
            );
        }
        return result;
    }
}
