/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2005-2011 Julian Hyde and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.StarColumnPredicate;

import java.util.*;

/**
 * Collection of values of one of the columns that parameterizes a
 * {@link Segment}.
 *
 * @version $Id$
 */
public class SegmentAxis {

    /**
     * Constraint on the keys in this Axis. Never null.
     */
    final StarColumnPredicate predicate;

    /**
     * Map holding the position of each key value.
     *
     * <p>TODO: Hold keys in a sorted array, then deduce ordinal by doing
     * binary search.
     */
    private final Map<Comparable<?>, Integer> mapKeyToOffset =
        new HashMap<Comparable<?>, Integer>();

    /**
     * Actual key values retrieved.
     */
    private final Comparable<?>[] keys;

    private static final Integer ZERO = Integer.valueOf(0);
    private static final Integer ONE = Integer.valueOf(1);

    /**
     * Internal constructor.
     */
    private SegmentAxis(
        StarColumnPredicate predicate,
        Comparable[] keys,
        boolean safe)
    {
        this.predicate = predicate;
        this.keys = keys;
        for (int i = 0; i < keys.length; i++) {
            mapKeyToOffset.put(keys[i], i);
        }
        assert predicate != null;
        assert safe || Util.isSorted(Arrays.asList(keys));
    }

    /**
     * Creates a SegmentAxis populated with an array of key values. The key
     * values must be sorted.
     *
     * @param predicate Predicate defining which keys should appear on
     *                  axis. (If a key passes the predicate but
     *                  is not in the list, every cell with that
     *                  key is assumed to have a null value.)
     * @param keys      Keys
     */
    SegmentAxis(StarColumnPredicate predicate, Comparable[] keys) {
        this(predicate, keys, false);
    }

    /**
     * Creates a SegmentAxis populated with a set of key values.
     *
     * @param predicate Predicate defining which keys should appear on
     *                  axis. (If a key passes the predicate but
     *                  is not in the list, every cell with that
     *                  key is assumed to have a null value.)
     * @param keySet Set of distinct key values, sorted
     * @param hasNull  Whether the axis contains the null value, in addition
     *                 to the values in <code>valueSet</code>
     */
    public SegmentAxis(
        StarColumnPredicate predicate,
        SortedSet<Comparable<?>> keySet,
        boolean hasNull)
    {
        this(predicate, toArray(keySet, hasNull), true);
    }

    private static Comparable<?>[] toArray(
        SortedSet<Comparable<?>> keySet,
        boolean hasNull)
    {
        int size = keySet.size();
        if (hasNull) {
            size++;
        }
        Comparable<?>[] keys = keySet.toArray(new Comparable<?>[size]);
        if (hasNull) {
            keys[size - 1] = RolapUtil.sqlNullValue;
        }
        return keys;
    }

    final StarColumnPredicate getPredicate() {
        return predicate;
    }

    final Comparable<?>[] getKeys() {
        return keys;
    }

    static Comparable wrap(Object o) {
        // Before JDK 1.5, Boolean did not implement Comparable
        if (Util.PreJdk15 && o instanceof Boolean) {
            return (Boolean) o ? ONE : ZERO;
        } else {
            return (Comparable) o;
        }
    }

    final int getOffset(Object o) {
        return getOffset(wrap(o));
    }

    final int getOffset(Comparable key) {
        Integer ordinal = mapKeyToOffset.get(key);
        if (ordinal == null) {
            return -1;
        }
        return ordinal;
    }

    /**
     * Returns whether this axis contains a given key, or would contain it
     * if it existed.
     *
     * <p>For example, if this axis is unconstrained, then this method
     * returns <code>true</code> for any value.
     *
     * @param key Key
     * @return Whether this axis would contain <code>key</code>
     */
    boolean contains(Object key) {
        return predicate.evaluate(key);
    }

    /**
     * Returns how many of this SegmentAxis's keys match a given constraint.
     *
     * @param predicate Predicate
     * @return How many keys match constraint
     */
    public int getMatchCount(StarColumnPredicate predicate) {
        int matchCount = 0;
        for (Object key : keys) {
            if (predicate.evaluate(key)) {
                ++matchCount;
            }
        }
        return matchCount;
    }
}

// End SegmentAxis.java
