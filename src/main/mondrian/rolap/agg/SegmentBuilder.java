/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.agg.Segment.ExcludedRegion;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.ConstrainedColumn;
import mondrian.spi.SegmentHeader;
import mondrian.util.ArraySortedSet;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

/**
 * This helper class contains methods to convert between
 * {@link Segment} and {@link SegmentHeader}, and also
 * {@link SegmentWithData}.
 * @author LBoudreau
 */
public class SegmentBuilder {
    /**
     * Converts a segment plus a {@link mondrian.rolap.agg.SegmentBody} into a
     * {@link mondrian.rolap.agg.SegmentWithData}.
     *
     * @param segment Segment
     * @param sb Segment body
     * @return SegmentWithData
     */
    public static SegmentWithData addData(Segment segment, SegmentBody sb) {
        // Load the axis keys for this segment
        SegmentAxis[] axes =
            new SegmentAxis[segment.predicates.length];
        for (int i = 0; i < segment.predicates.length; i++) {
            StarColumnPredicate predicate =
                segment.predicates[i];
            axes[i] =
                new SegmentAxis(
                    predicate,
                    sb.getAxisValueSets()[i],
                    sb.getNullAxisFlags()[i]);
        }
        final SegmentDataset dataSet =
            sb.createSegmentDataset(segment, axes);
        return new SegmentWithData(segment, dataSet, axes);
    }

    /**
     * Creates a segment from a SegmentHeader. The star,
     * constrainedColsBitKey, constrainedColumns and measure arguments are a
     * helping hand, because we know what we were looking for.
     *
     * @param header The header to convert.
     * @param star Star
     * @param constrainedColumnsBitKey Constrained columns
     * @param constrainedColumns Constrained columns
     * @param measure Measure
     * @return Segment
     */
    public static Segment toSegment(
        SegmentHeader header,
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        RolapStar.Column[] constrainedColumns,
        RolapStar.Measure measure,
        List<StarPredicate> compoundPredicates)
    {
        final List<StarColumnPredicate> predicateList =
            new ArrayList<StarColumnPredicate>();
        for (int i = 0; i < constrainedColumns.length; i++) {
            RolapStar.Column constrainedColumn = constrainedColumns[i];
            final SortedSet<Comparable<?>> values =
                header.getConstrainedColumns()[i].values;
            StarColumnPredicate predicate;
            if (values == null) {
                predicate =
                    new LiteralStarPredicate(
                        constrainedColumn,
                        true);
            } else if (values.size() == 1) {
                predicate =
                    new ValueColumnPredicate(
                        constrainedColumn,
                        values.first());
            } else {
                final List<StarColumnPredicate> valuePredicateList =
                    new ArrayList<StarColumnPredicate>();
                for (Object value : values) {
                    valuePredicateList.add(
                        new ValueColumnPredicate(
                            constrainedColumn,
                            value));
                }
                predicate =
                    new ListColumnPredicate(
                        constrainedColumn,
                        valuePredicateList);
            }
            predicateList.add(predicate);
        }

        return new Segment(
            star,
            constrainedColumnsBitKey,
            constrainedColumns,
            measure,
            predicateList.toArray(
                new StarColumnPredicate[predicateList.size()]),
            new ExcludedRegionList(header),
            compoundPredicates);
    }

    private static class ExcludedRegionList
        extends AbstractList<Segment.ExcludedRegion>
        implements Segment.ExcludedRegion
    {
        private final int cellCount;
        private final SegmentHeader header;
        public ExcludedRegionList(SegmentHeader header) {
            this.header = header;
            int cellCount = 1;
            for (ConstrainedColumn cc : header.getExcludedRegions()) {
                // TODO find a way to approximate the cardinality
                // of wildcard columns.
                if (cc.values != null) {
                    cellCount *= cc.values.size();
                }
            }
            this.cellCount = cellCount;
        }
        public void describe(StringBuilder buf) {
            // TODO
        }
        public int getArrity() {
            return header.getConstrainedColumns().length;
        }
        public int getCellCount() {
            return cellCount;
        }
        public boolean wouldContain(Object[] keys) {
            assert keys.length == header.getConstrainedColumns().length;
            for (int i = 0; i < keys.length; i++) {
                final ConstrainedColumn excl =
                    header.getExcludedRegion(
                        header.getConstrainedColumns()[i].columnExpression);
                if (excl == null) {
                    continue;
                }
                if (excl.values.contains(keys[i])) {
                    return true;
                }
            }
            return false;
        }
        public ExcludedRegion get(int index) {
            return this;
        }
        public int size() {
            return 1;
        }
    }

    /**
     * Tells if the passed segment is a subset of this segment
     * and could be used for a rollup in cache operation.
     * @param segment A segment which might be a subset of the
     * current segment.
     * @return True or false.
     */
    public static boolean isSubset(
        SegmentHeader header,
        Segment segment)
    {
        if (!segment.getStar().getSchema().getName()
            .equals(header.schemaName))
        {
            return false;
        }
        if (!segment.getStar().getFactTable().getAlias()
                .equals(header.rolapStarFactTableName))
        {
            return false;
        }
        if (!segment.measure.getName().equals(header.measureName)) {
            return false;
        }
        if (!segment.measure.getCubeName().equals(header.cubeName)) {
            return false;
        }
        if (segment.getConstrainedColumnsBitKey()
                .equals(header.constrainedColsBitKey))
        {
            return true;
        }
        return false;
    }

    public static ConstrainedColumn[] toConstrainedColumns(
        StarColumnPredicate[] predicates)
    {
        return toConstrainedColumns(
            Arrays.asList(predicates));
    }

    public static ConstrainedColumn[] toConstrainedColumns(
        Collection<StarColumnPredicate> predicates)
    {
        List<ConstrainedColumn> ccs =
            new ArrayList<ConstrainedColumn>();
        for (StarColumnPredicate predicate : predicates) {
            final List<Comparable<?>> values =
                new ArrayList<Comparable<?>>();
            predicate.values(Util.cast(values));
            final Comparable<?>[] valuesArray =
                values.toArray(new Comparable<?>[values.size()]);
            if (valuesArray.length == 1 && valuesArray[0].equals(true)) {
                ccs.add(
                    new ConstrainedColumn(
                        predicate.getConstrainedColumn()
                            .getExpression().getGenericExpression(),
                        null));
            } else {
                Arrays.sort(valuesArray);
                ccs.add(
                    new ConstrainedColumn(
                        predicate.getConstrainedColumn()
                        .getExpression().getGenericExpression(),
                        new ArraySortedSet(valuesArray)));
            }
        }
        return ccs.toArray(new ConstrainedColumn[ccs.size()]);
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     *
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader toHeader(Segment segment) {
        final ConstrainedColumn[] cc =
            SegmentBuilder.toConstrainedColumns(segment.predicates);
        final List<String> cp = new ArrayList<String>();

        StringBuilder buf = new StringBuilder();

        for (StarPredicate compoundPredicate : segment.compoundPredicateList) {
            buf.setLength(0);
            SqlQuery query =
                new SqlQuery(
                    segment.star.getSqlQueryDialect());
            compoundPredicate.toSql(query, buf);
            cp.add(buf.toString());
        }
        final RolapSchema schema = segment.star.getSchema();
        return new SegmentHeader(
            schema.getName(),
            schema.getChecksum(),
            segment.measure.getCubeName(),
            segment.measure.getName(),
            cc,
            cp.toArray(new String[cp.size()]),
            segment.star.getFactTable().getAlias(),
            segment.constrainedColumnsBitKey);
    }
}

// End SegmentBuilder.java
