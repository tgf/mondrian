/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.*;

/**
 * A <code>Segment</code> is a collection of cell values parameterized by
 * a measure, and a set of (column, value) pairs. An example of a segment is</p>
 *
 * <blockquote>
 *   <p>(Unit sales, Gender = 'F', State in {'CA','OR'}, Marital Status = <i>
 *   anything</i>)</p>
 * </blockquote>
 *
 * <p>All segments over the same set of columns belong to an Aggregation, in
 * this case:</p>
 *
 * <blockquote>
 *   <p>('Sales' Star, Gender, State, Marital Status)</p>
 * </blockquote>
 *
 * <p>Note that different measures (in the same Star) occupy the same
 * Aggregation.  Aggregations belong to the AggregationManager, a singleton.</p>
 *
 * <p>Segments are pinned during the evaluation of a single MDX query. The query
 * evaluates the expressions twice. The first pass, it finds which cell values
 * it needs, pins the segments containing the ones which are already present
 * (one pin-count for each cell value used), and builds a {@link CellRequest
 * cell request} for those which are not present. It executes the cell request
 * to bring the required cell values into the cache, again, pinned. Then it
 * evalutes the query a second time, knowing that all cell values are
 * available. Finally, it releases the pins.</p>
 *
 * <p>A Segment may have a list of excluded {@link Region} objects. These are
 * caused by cache flushing. Usually a segment is a hypercube: it is defined by
 * a set of values on each of its axes. But after a cache flush request, a
 * segment may have a rectangular 'hole', and therefore not be a hypercube
 * anymore.
 *
 * <p>For example, the segment defined by {CA, OR, WA} * {F, M} is a
 * 2-dimensional hyper-rectangle with 6 cells. After flushing {CA, OR, TX} *
 * {F}, the result is 4 cells:
 *
 * <pre>
 *     F     M
 * CA  out   in
 * OR  out   in
 * WA  in    in
 * </pre>
 *
 * defined by the original segment minus the region ({CA, OR} * {F}).
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
public class Segment {
    private static int nextId = 0; // generator for "id"

    final int id; // for debug
    private String desc;

    /**
     * This is set in the load method and is used during
     * the processing of a particular aggregate load.
     */
    protected final RolapStar.Column[] columns;

    final RolapStar.Measure measure;

    /**
     * An array of axes, one for each constraining column, containing the values
     * returned for that constraining column.
     */
    final StarColumnPredicate[] predicates;

    protected final RolapStar star;
    protected final BitKey constrainedColumnsBitKey;

    /**
     * List of regions to ignore when reading this segment. This list is
     * populated when a region is flushed. The cells for these regions may be
     * physically in the segment, because trimming segments can be expensive,
     * but should still be ignored.
     */
    protected final List<Region> excludedRegions;

    private final int aggregationKeyHashCode;
    protected final List<StarPredicate> compoundPredicateList;

    private static final Logger LOGGER = Logger.getLogger(Segment.class);

    /**
     * Creates a <code>Segment</code>; it's not loaded yet.
     *
     * @param star Star that this Segment belongs to
     * @param measure Measure whose values this Segment contains
     * @param predicates List of predicates constraining each axis
     * @param excludedRegions List of regions which are not in this segment.
     */
    public Segment(
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        RolapStar.Column[] columns,
        RolapStar.Measure measure,
        StarColumnPredicate[] predicates,
        List<Region> excludedRegions,
        final List<StarPredicate> compoundPredicateList)
    {
        this.id = nextId++;
        this.star = star;
        this.constrainedColumnsBitKey = constrainedColumnsBitKey;
        this.columns = columns;
        this.measure = measure;
        this.predicates = predicates;
        this.excludedRegions = excludedRegions;
        for (Region region : excludedRegions) {
            assert region.getPredicates().size() == predicates.length;
        }
        this.compoundPredicateList = compoundPredicateList;
        final List<BitKey> compoundPredicateBitKeys =
            compoundPredicateList == null
                ? null
                : new AbstractList<BitKey>() {
                    public BitKey get(int index) {
                        return compoundPredicateList.get(index)
                            .getConstrainedColumnBitKey();
                    }

                    public int size() {
                        return compoundPredicateList.size();
                    }
                };
        this.aggregationKeyHashCode =
            AggregationKey.computeHashCode(
                constrainedColumnsBitKey,
                star,
                compoundPredicateBitKeys);
    }

    /**
     * Returns the constrained columns.
     */
    public RolapStar.Column[] getColumns() {
        return columns;
    }

    /**
     * Returns the star.
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Returns the BitKey for ALL columns (Measures and Levels) involved in the
     * query.
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    private void describe(StringBuilder buf, boolean values) {
        final String sep = Util.nl + "    ";
        buf.append(printSegmentHeaderInfo(sep));

        for (int i = 0; i < columns.length; i++) {
            buf.append(sep);
            buf.append(columns[i].getExpression().getGenericExpression());
            describeAxes(buf, i, values);
        }
        if (!excludedRegions.isEmpty()) {
            buf.append(sep);
            buf.append("excluded={");
            int k = 0;
            for (Region excludedRegion : excludedRegions) {
                if (k++ > 0) {
                    buf.append(", ");
                }
                excludedRegion.describe(buf);
            }
            buf.append('}');
        }
        buf.append('}');
    }

    protected void describeAxes(StringBuilder buf, int i, boolean values) {
        predicates[i].describe(buf);
    }

    private String printSegmentHeaderInfo(String sep) {
        StringBuilder buf = new StringBuilder();
        buf.append("Segment #");
        buf.append(id);
        buf.append(" {");
        buf.append(sep);
        buf.append("measure=");
        buf.append(
            measure.getExpression() == null
                ? measure.getAggregator().getExpression("*")
                : measure.getAggregator().getExpression(
                    measure.getExpression().getGenericExpression()));
        return buf.toString();
    }

    public String toString() {
        if (this.desc == null) {
            StringBuilder buf = new StringBuilder(64);
            describe(buf, false);
            this.desc = buf.toString();
        }
        return this.desc;
    }

    /**
     * Returns whether a cell value is excluded from this segment.
     */
    protected final boolean isExcluded(Object[] keys) {
        // Performance critical: cannot use foreach
        final int n = excludedRegions.size();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < n; i++) {
            Region excludedRegion = excludedRegions.get(i);
            if (excludedRegion.wouldContain(keys)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prints the state of this <code>Segment</code>, including constraints
     * and values. Blocks the current thread until the segment is loaded.
     *
     * @param pw Writer
     */
    public void print(PrintWriter pw) {
        final StringBuilder buf = new StringBuilder();
        describe(buf, true);
        pw.print(buf.toString());
        pw.println();
    }

    public List<Region> getExcludedRegions() {
        return excludedRegions;
    }

    SegmentDataset createDataset(
        SegmentAxis[] axes,
        boolean sparse,
        SqlStatement.Type type,
        int size)
    {
        if (sparse) {
            return new SparseSegmentDataset();
        } else {
            switch (type) {
            case OBJECT:
            case STRING:
                return new DenseObjectSegmentDataset(axes, size);
            case INT:
                return new DenseIntSegmentDataset(axes, size);
            case DOUBLE:
                return new DenseDoubleSegmentDataset(axes, size);
            default:
                throw Util.unexpected(type);
            }
        }
    }

    public boolean matches(
        AggregationKey aggregationKey,
        RolapStar.Measure measure)
    {
        // Perform high-selectivity comparisons first.
        return aggregationKeyHashCode == aggregationKey.hashCode()
            && this.measure == measure
            && matchesInternal(aggregationKey);
    }

    private boolean matchesInternal(AggregationKey aggKey) {
        return
            constrainedColumnsBitKey.equals(
                aggKey.getConstrainedColumnsBitKey())
            && star.equals(aggKey.getStar())
            && AggregationKey.equal(
                compoundPredicateList,
                aggKey.compoundPredicateList);
    }

    /**
     * Definition of a region of values which are not in a segment.
     *
     * <p>A region is defined by a set of constraints, one for each column
     * in the segment. A constraint may be
     * {@link mondrian.rolap.agg.LiteralStarPredicate}(true), meaning that
     * the column is unconstrained.
     *
     * <p>For example,
     * <pre>
     * segment (State={CA, OR, WA}, Gender=*)
     * actual values {1997, 1998} * {CA, OR, WA} * {M, F} = 12 cells
     * excluded region (Year=*, State={CA, OR}, Gender={F})
     * excluded values {1997, 1998} * {CA, OR} * {F} = 4 cells
     *
     * Values:
     *
     *     F     M
     * CA  out   in
     * OR  out   in
     * WA  in    in
     * </pre>
     *
     * <p>Note that the resulting segment is not a hypercube: it has a 'hole'.
     * This is why regions are required.
     */
    static class Region {
        private final StarColumnPredicate[] predicates;
        private final StarPredicate[] multiColumnPredicates;
        protected final int cellCount;

        Region(
            List<StarColumnPredicate> predicateList,
            List<StarPredicate> multiColumnPredicateList,
            int cellCount)
        {
            this.predicates =
                predicateList.toArray(
                    new StarColumnPredicate[predicateList.size()]);
            this.multiColumnPredicates =
                multiColumnPredicateList.toArray(
                    new StarPredicate[multiColumnPredicateList.size()]);
            this.cellCount = cellCount;
        }

        public List<StarColumnPredicate> getPredicates() {
            return Collections.unmodifiableList(Arrays.asList(predicates));
        }

        public List<StarPredicate> getMultiColumnPredicates() {
            return Collections.unmodifiableList(
                Arrays.asList(multiColumnPredicates));
        }

        public int getCellCount() {
            return cellCount;
        }

        public boolean wouldContain(Object[] keys) {
            assert keys.length == predicates.length;
            for (int i = 0; i < keys.length; i++) {
                final Object key = keys[i];
                final StarColumnPredicate predicate = predicates[i];
                if (!predicate.evaluate(key)) {
                    return false;
                }
            }
            return true;
        }

        public boolean equals(Object obj) {
            if (obj instanceof Region) {
                Region that = (Region) obj;
                return Arrays.equals(
                    this.predicates, that.predicates)
                    && Arrays.equals(
                        this.multiColumnPredicates,
                        that.multiColumnPredicates);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Arrays.hashCode(multiColumnPredicates) ^
                Arrays.hashCode(predicates);
        }

        /**
         * Describes this Segment.
         * @param buf Buffer to write to.
         */
        public void describe(StringBuilder buf) {
            int k = 0;
            for (StarColumnPredicate predicate : predicates) {
                if (predicate instanceof LiteralStarPredicate
                    && ((LiteralStarPredicate) predicate).getValue())
                {
                    continue;
                }
                if (k++ > 0) {
                    buf.append(" AND ");
                }
                predicate.describe(buf);
            }
            for (StarPredicate predicate : multiColumnPredicates) {
                if (predicate instanceof LiteralStarPredicate
                    && ((LiteralStarPredicate) predicate).getValue())
                {
                    continue;
                }
                if (k++ > 0) {
                    buf.append(" AND ");
                }
                predicate.describe(buf);
            }
        }
    }
}

// End Segment.java
