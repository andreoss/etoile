/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile.discrepancy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BinaryOperator;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.cactoos.iterator.IteratorOf;
import scala.Tuple2;

/**
 * Compare two rows.
 *
 * @since 0.7.0
 */
@RequiredArgsConstructor
public final class Compare implements FlatMapFunction<Tuple2<Row, Row>, Row>, BinaryOperator<Row> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -2853296214692532016L;

    /**
     * Comparison.
     */
    private final Comparison comp;

    @Override
    public Iterator<Row> call(final Tuple2<Row, Row> joined) throws Exception {
        final Iterator<Row> result;
        final Row res = this.apply(joined._1(), joined._2());
        if (res == null) {
            result = new IteratorOf<>();
        } else {
            result = new IteratorOf<>(res);
        }
        return result;
    }

    @Override
    public Row apply(final Row fst, final Row snd) {
        final Row res;
        if (fst == null && snd != null) {
            res = Compare.result(snd, new Mismatch("left side is missing"));
        } else if (snd == null && fst != null) {
            res = Compare.result(fst, new Mismatch("right side is missing"));
        } else {
            Objects.requireNonNull(fst);
            Objects.requireNonNull(snd);
            final StructType fsc = fst.schema();
            final Collection<Outcome> result = new ArrayList<>(fsc.size());
            for (final String field : fsc.fieldNames()) {
                final Object fval = fst.getAs(field);
                final Object sval = snd.getAs(field);
                result.add(
                    new Detailed(
                        Compare.fieldDescription(field, fsc),
                        this.comp.make(fval, sval)
                    )
                );
            }
            final Outcome fin = new GroupOutcome(result);
            if (fin.isOkay()) {
                res = null;
            } else {
                res = Compare.result(fst, fin);
            }
        }
        return res;
    }

    /**
     * Result row.
     * @param fst Original row.
     * @param fin Outcome of check.
     * @return Resulting row.
     */
    private static Row result(final Row fst, final Outcome fin) {
        final Row res;
        final Object[] row = new Object[fst.schema().size()];
        fst.toSeq().copyToArray(row);
        row[fst.schema().fieldIndex("__result")] = fin.description();
        res = new GenericRowWithSchema(
            row, fst.schema()
        );
        return res;
    }

    /**
     * Describe field.
     * @param field Field.
     * @param fsc Main schema.
     * @return Description.
     */
    private static String fieldDescription(final String field, final StructType fsc) {
        return String.format("%s(%s):", field, fsc.apply(field).dataType().sql());
    }
}
