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
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.ByteString;

import java.io.Serializable;
import java.util.*;

/**
 * SegmentHeaders are the key objects used to retrieve the segments
 * from the segment cache.
 *
 * <p>The segment header objects are immutable and fully serializable.
 *
 * <p>The headers have each an ID which is a SHA-256 checksum of the
 * following properties, concatenated. See
 * {@link SegmentHeader#getUniqueID()}
 * <ul>
 * <li>Schema Name</li>
 * <li>Cube Name</li>
 * <li>Measure Name</li>
 * <li>For each column:</li>
 *   <ul>
 *   <li>Column table name</li>
 *   <li>Column physical name</li>
 *   <li>For each predicate value:</li>
 *     <ul>
 *     <li>The equivalent of
 *     <code>String.valueof([value object])</code></li>
 *     </ul>
 *   </ul>
 * </ul>
 *
 * @author LBoudreau
 * @version $Id$
 */
public class SegmentHeader implements Serializable {
    private static final long serialVersionUID = 8696439182886512850L;
    private final int arity;
    private final ConstrainedColumn[] constrainedColumns;
    private final String[] compoundPredicates;
    public final String measureName;
    public final String cubeName;
    public final String schemaName;
    public final String rolapStarFactTableName;
    public final BitKey constrainedColsBitKey;
    private final int hashCode;
    private ByteString uniqueID;
    private String description;
    public final ByteString schemaChecksum;

    /**
     * Base constructor for segment headers.
     *
     * @param schemaName The name of the schema which this
     * header belongs to.
     * @param cubeName The name of the cube this segment belongs to.
     * @param measureName The name of the measure which defines
     * this header.
     * @param constrainedColumns An array of constrained columns
     * objects which define the predicated of this segment header.
     */
    public SegmentHeader(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        ConstrainedColumn[] constrainedColumns,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey)
    {
        this(
            schemaName, schemaChecksum, cubeName, measureName,
            constrainedColumns, new String[0],
            rolapStarFactTableName, constrainedColsBitKey);
    }

    /**
     * Base constructor for segment headers.
     *
     * @param schemaName The name of the schema which this
     * header belongs to.
     * @param cubeName The name of the cube this segment belongs to.
     * @param measureName The name of the measure which defines
     * this header.
     * @param constrainedColumns An array of constrained columns
     * objects which define the predicated of this segment header.
     */
    public SegmentHeader(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        ConstrainedColumn[] constrainedColumns,
        String[] compoundPredicates,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey)
    {
        this.constrainedColumns = constrainedColumns;
        this.schemaName = schemaName;
        this.schemaChecksum = schemaChecksum;
//        assert schemaChecksum != null;
        this.cubeName = cubeName;
        this.measureName = measureName;
        this.compoundPredicates = compoundPredicates;
        this.rolapStarFactTableName = rolapStarFactTableName;
        this.constrainedColsBitKey = constrainedColsBitKey;
        this.arity = constrainedColumns.length;
        // Hash code might be used extensively. Better compute
        // it up front. Make sure the columns are ordered in a
        // deterministic order (alpha...)
        int hash = 42;
        hash = Util.hash(hash, schemaName);
        hash = Util.hash(hash, schemaChecksum);
        hash = Util.hash(hash, cubeName);
        hash = Util.hash(hash, measureName);
        for (ConstrainedColumn col : this.constrainedColumns) {
            hash = Util.hash(hash, col.columnExpression);
            hash = Util.hashArray(hash, col.values);
        }
        for (String col : this.compoundPredicates) {
            hash = Util.hash(hash, col);
        }
        this.hashCode = hash;
    }

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

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentHeader)) {
            return false;
        }
        return ((SegmentHeader)obj).getUniqueID().equals(this.getUniqueID());
    }

    public static ConstrainedColumn[] forCacheRegion(
        RolapCacheRegion region)
    {
        final ConstrainedColumn[] cc =
            new ConstrainedColumn[region.getColumnPredicates().size()];
        int i = 0;
        for (StarColumnPredicate predicate : region.getColumnPredicates()) {
            // First get the values
            final List<Object> values = new ArrayList<Object>();
            predicate.values(values);
            // Now build the CC object
            cc[i] =
                new SegmentHeader.ConstrainedColumn(
                    predicate.getConstrainedColumn()
                        .getExpression().getGenericExpression(),
                    values.toArray());
            i++;
        }
        return cc;
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     *
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader forSegment(
        Segment segment,
        List<StarPredicate> compoundPredicates)
    {
        Util.deprecated(
            "remove compoundPredicates parameter - duplicates segment field",
            false);
        final List<ConstrainedColumn> cc =
            new ArrayList<ConstrainedColumn>();
        final List<String> cp = new ArrayList<String>();
        for (StarColumnPredicate predicate : segment.predicates) {
            cc.add(toConstrainedColumn(predicate));
        }
        StringBuilder buf = new StringBuilder();
        for (StarPredicate compoundPredicate : compoundPredicates) {
            buf.setLength(0);
            SqlQuery query =
                new SqlQuery(
                    segment.getStar().getSqlQueryDialect());
            compoundPredicate.toSql(query, buf);
            cp.add(buf.toString());
        }
        final RolapStar.Measure measure = segment.measure;
        final RolapStar star = measure.getStar();
        final RolapSchema schema = star.getSchema();
        return new SegmentHeader(
            schema.getName(),
            schema.getChecksum(),
            measure.getCubeName(),
            measure.getName(),
            cc.toArray(new ConstrainedColumn[cc.size()]),
            cp.toArray(new String[cp.size()]),
            star.getFactTable().getAlias(),
            segment.getConstrainedColumnsBitKey());
    }

    private static ConstrainedColumn toConstrainedColumn(
        StarColumnPredicate predicate)
    {
        if (predicate instanceof LiteralStarPredicate
            && predicate.evaluate(Collections.emptyList()))
        {
            // Column is not constrained, i.e. wildcard.
            return new ConstrainedColumn(
                predicate.getConstrainedColumn()
                    .getExpression().getGenericExpression(),
                null);
        }

        final List<Object> values = new ArrayList<Object>();
        predicate.values(values);
        return new ConstrainedColumn(
            predicate.getConstrainedColumn()
                .getExpression().getGenericExpression(),
            values.toArray());
    }

    /**
     * Creates a segment from this SegmentHeader. The star,
     * constrainedColsBitKey, constrainedColumns and measure arguments are a
     * helping hand, because we know what we were looking for.
     *
     * @param star Star
     * @param constrainedColumnsBitKey Constrained columns
     * @param constrainedColumns Constrained columns
     * @param measure Measure
     * @return Segment
     */
    public Segment toSegment(
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        RolapStar.Column[] constrainedColumns,
        RolapStar.Measure measure)
    {
        // TODO: read excludedRegions, compoundPredicateList
        // from the SegmentHeader
        final List<StarColumnPredicate> predicateList =
            new ArrayList<StarColumnPredicate>();
        for (int i = 0; i < constrainedColumns.length; i++) {
            RolapStar.Column constrainedColumn = constrainedColumns[i];
            final Object[] values = this.constrainedColumns[i].values;
            StarColumnPredicate predicate;
            if (values == null) {
                predicate =
                    new LiteralStarPredicate(
                        constrainedColumn,
                        true);
            } else if (values.length == 1) {
                predicate =
                    new ValueColumnPredicate(
                        constrainedColumn,
                        values[0]);
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
        List<Segment.Region> excludedRegions = Collections.emptyList();
        List<StarPredicate> compoundPredicateList = Collections.emptyList();
        return new Segment(
            star,
            constrainedColumnsBitKey,
            constrainedColumns,
            measure,
            predicateList.toArray(
                new StarColumnPredicate[predicateList.size()]),
            excludedRegions,
            compoundPredicateList);
    }

    /**
     * Creates a clone of this header by replacing some of the
     * constrained columns in the process.
     * @param overrideValues A list of constrained columns to either
     * replace or add to the original header.
     * @return A clone of the header with the columns replaced.
     */
    public SegmentHeader clone(ConstrainedColumn[] overrideValues) {
        Map<String, ConstrainedColumn> colsToAdd =
            new HashMap<String, ConstrainedColumn>();
        for (ConstrainedColumn cc : this.constrainedColumns) {
            colsToAdd.put(cc.columnExpression, cc);
        }
        for (ConstrainedColumn override : overrideValues) {
            colsToAdd.put(override.columnExpression, override);
        }
        return
            new SegmentHeader(
                schemaName,
                schemaChecksum,
                cubeName,
                measureName,
                colsToAdd.values()
                    .toArray(new ConstrainedColumn[colsToAdd.size()]),
                rolapStarFactTableName,
                constrainedColsBitKey);
    }

    /**
     * Constrained columns are part of the SegmentHeader and SegmentCache.
     * They uniquely identify a constrained column within a segment.
     * Each segment can have many constrained columns. Each column can
     * be constrained by multiple values at once (similar to a SQL in()
     * predicate).
     *
     * <p>They are immutable and serializable.
     */
    public static class ConstrainedColumn implements Serializable {
        private static final long serialVersionUID = -5227838916517784720L;
        final String columnExpression;
        final Object[] values;
        private int hashCode = Integer.MIN_VALUE;

        /**
         * Creates a ConstrainedColumn.
         *
         * @param columnExpression Name of the source table into which the
         * constrained column is, as defined in the Mondrian schema.
         *
         * @param valueList List of values to constrain the
         *     column to, or null if unconstrained. Values must be
         *     {@link Comparable} and immutable. For example, Integer, Boolean,
         *     String or Double.
         */
        public ConstrainedColumn(
            String columnExpression,
            Object[] valueList)
        {
            this.columnExpression = columnExpression;
            this.values = valueList == null ? null : valueList.clone();
        }

        /**
         * Returns the column expression of this constrained column.
         * @return A column expression.
         */
        public String getColumnExpression() {
            return columnExpression;
        }

        /**
         * Returns an array of predicate values for this column.
         * @return An array of object values.
         */
        public Object[] getValues() {
            return values;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ConstrainedColumn)) {
                return false;
            }
            ConstrainedColumn that = (ConstrainedColumn) obj;
            return this.columnExpression.equals(that.columnExpression)
                && Arrays.equals(this.values, that.values);
        }

        @Override
        public int hashCode() {
            if (this.hashCode  == Integer.MIN_VALUE) {
                int hash = super.hashCode();
                hash = Util.hash(hash, this.columnExpression);
                for (Object val : this.values) {
                    hash = Util.hash(hash, val);
                }
                this.hashCode = hash;
            }
            return hashCode;
        }
    }

    public String toString() {
        return this.getDescription();
    }

    /**
     * Returns the arity of this SegmentHeader.
     * @return The arity as an integer number.
     */
    public int getArity() {
        return arity;
    }

    /**
     * Returns an array of constrained columns which define this segment
     * header. The caller should consider this list immutable.
     * @return An array of ConstrainedColumns
     */
    public ConstrainedColumn[] getConstrainedColumns() {
        ConstrainedColumn[] copy =
            new ConstrainedColumn[this.constrainedColumns.length];
        System.arraycopy(
            constrainedColumns,
            0,
            copy,
            0,
            constrainedColumns.length);
        return copy;
    }

    /**
     * Returns the constrained column object, if any, corresponding
     * to a column name and a table name.
     * @param columnExpression The column name we want.
     * @return A Constrained column, or null.
     */
    public ConstrainedColumn getConstrainedColumn(
        String columnExpression)
    {
        for (ConstrainedColumn c : constrainedColumns) {
            if (c.columnExpression.equals(columnExpression)) {
                return c;
            }
        }
        return null;
    }

    public BitKey getConstrainedColumnsBitKey() {
        return this.constrainedColsBitKey.copy();
    }

    /**
     * Tells if the passed segment is a subset of this segment
     * and could be used for a rollup in cache operation.
     * @param segment A segment which might be a subset of the
     * current segment.
     * @return True or false.
     */
    public boolean isSubset(Segment segment) {
        if (!segment.getStar().getSchema().getName().equals(schemaName)) {
            return false;
        }
        if (!segment.getStar().getFactTable().getAlias()
                .equals(rolapStarFactTableName))
        {
            return false;
        }
        if (!segment.measure.getName().equals(measureName)) {
            return false;
        }
        if (!segment.measure.getCubeName().equals(cubeName)) {
            return false;
        }
        if (segment.getConstrainedColumnsBitKey()
                .equals(constrainedColsBitKey))
        {
            return true;
        }
        return false;
    }

    /**
     * Returns a unique identifier for this header. The identifier
     * can be used for storage and will be the same across segments
     * which have the same schema name, cube name, measure name,
     * and for each constrained column, the same column name, table name,
     * and predicate values.
     * @return A unique identification string.
     */
    public ByteString getUniqueID() {
        if (this.uniqueID == null) {
            StringBuilder hashSB = new StringBuilder();
            hashSB.append(this.schemaName);
            hashSB.append(this.schemaChecksum);
            hashSB.append(this.cubeName);
            hashSB.append(this.measureName);
            for (ConstrainedColumn c : constrainedColumns) {
                hashSB.append(c.columnExpression);
                if (c.values != null) {
                    for (Object value : c.values) {
                        hashSB.append(String.valueOf(value));
                    }
                }
            }
            for (String c : compoundPredicates) {
                hashSB.append(c);
            }
            this.uniqueID =
                new ByteString(Util.digestSha256(hashSB.toString()));
        }
        return uniqueID;
    }

    /**
     * Returns a human readable description of this
     * segment header.
     * @return A string describing the header.
     */
    public String getDescription() {
        if (this.description == null) {
            StringBuilder descriptionSB = new StringBuilder();
            descriptionSB.append("*Segment Header\n");
            descriptionSB.append("Schema:[");
            descriptionSB.append(this.schemaName);
            descriptionSB.append("]\nChecksum:[");
            descriptionSB.append(this.schemaChecksum);
            descriptionSB.append("]\nCube:[");
            descriptionSB.append(this.cubeName);
            descriptionSB.append("]\nMeasure:[");
            descriptionSB.append(this.measureName);
            descriptionSB.append("]\n");
            descriptionSB.append("Axes:[");
            for (ConstrainedColumn c : constrainedColumns) {
                descriptionSB.append("\n\t{");
                descriptionSB.append(c.columnExpression);
                descriptionSB.append("=(");
                for (Object value : c.values) {
                    descriptionSB.append("'");
                    descriptionSB.append(value);
                    descriptionSB.append("',");
                }
                descriptionSB.deleteCharAt(descriptionSB.length() - 1);
                descriptionSB.append(")}");
            }
            descriptionSB.append("]\n");
            descriptionSB.append("Compound Predicates:[");
            for (String c : compoundPredicates) {
                descriptionSB.append("\n\t{");
                descriptionSB.append(c);
            }
            descriptionSB
                .append("]\n")
                .append("ID:[")
                .append(getUniqueID())
                .append("]\n");
            this.description = descriptionSB.toString();
        }
        return description;
    }
}

// End SegmentHeader.java
