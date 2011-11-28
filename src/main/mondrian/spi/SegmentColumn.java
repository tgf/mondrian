/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.util.ArraySortedSet;

import java.io.Serializable;
import java.util.SortedSet;


/**
 * Constrained columns are part of the SegmentHeader and SegmentCache.
 * They uniquely identify a constrained column within a segment.
 * Each segment can have many constrained columns. Each column can
 * be constrained by multiple values at once (similar to a SQL in()
 * predicate).
 *
 * <p>They are immutable and serializable.
 *
 * @version $Id$
 */
public class SegmentColumn implements Serializable {
    private static final long serialVersionUID = -5227838916517784720L;
    public final String columnExpression;
    public final SortedSet<Comparable<?>> values;
    private final int hashCode;

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
    public SegmentColumn(
        String columnExpression,
        SortedSet<Comparable<?>> valueList)
    {
        this.columnExpression = columnExpression;
        this.values = valueList;
        int hash = super.hashCode();
        hash = Util.hash(hash, this.columnExpression);
        if (this.values != null) {
            for (Object val : this.values) {
                hash = Util.hash(hash, val);
            }
        }
        this.hashCode = hash;
    }

    /**
     * Merged the current constrained column with another
     * resulting in a super set of both.
     */
    public SegmentColumn merge(SegmentColumn col) {
        assert col != null;
        assert col.columnExpression.equals(this.columnExpression);

        // If any values are wildcard, the merged result is a wildcard.
        if (this.values == null || col.values == null) {
            return new SegmentColumn(
                columnExpression,
                null);
        }

        return new SegmentColumn(
            columnExpression,
            ((ArraySortedSet)this.values).merge(
                (ArraySortedSet)col.values));
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
    public SortedSet<Comparable<?>> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentColumn)) {
            return false;
        }
        SegmentColumn that = (SegmentColumn) obj;
        if (this.values == null && that.values == null) {
            return true;
        }
        return this.columnExpression.equals(that.columnExpression)
            && Util.equals(this.values, that.values);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
// End SegmentColumn.java