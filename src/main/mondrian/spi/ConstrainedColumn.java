package mondrian.spi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mondrian.olap.Util;

/**
 * Constrained columns are part of the SegmentHeader and SegmentCache.
 * They uniquely identify a constrained column within a segment.
 * Each segment can have many constrained columns. Each column can
 * be constrained by multiple values at once (similar to a SQL in()
 * predicate).
 *
 * <p>They are immutable and serializable.
 */
public class ConstrainedColumn implements Serializable {
    private static final long serialVersionUID = -5227838916517784720L;
    public final String columnExpression;
    public final Object[] values;
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
     * Merged the current constrained column with another
     * resulting in a super set of both.
     */
    public ConstrainedColumn merge(ConstrainedColumn col) {
        if (!col.columnExpression.equals(this.columnExpression)) {
            return this;
        }
        final List<Object> ccMergedValues =
            new ArrayList<Object>();
        // If any values are wildcard, the merged result is a wildcard.
        if (this.values == null || col.values == null) {
            return new ConstrainedColumn(
                columnExpression,
                null);
        }
        // Merge the values by hash/equality.
        ccMergedValues.addAll(Arrays.asList(this.values));
        for (Object value : col.values) {
            if (!ccMergedValues.contains(value)) {
                ccMergedValues.add(value);
            }
        }
        return new ConstrainedColumn(
            columnExpression,
            ccMergedValues.toArray());
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