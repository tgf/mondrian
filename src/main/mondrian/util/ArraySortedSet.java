/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

import java.util.*;

/**
 * Implementation of {@link java.util.SortedSet} based on an array. The array
 * must already be sorted in natural order.
 *
 * @param <E>
 *
 * @version $Id$
 * @author Julian Hyde
 */
public class ArraySortedSet<E extends Comparable<E>>
    extends AbstractSet<E>
    implements SortedSet<E>
{
    private final E[] values;
    private final int start;
    private final int end;

    /**
     * Creates a set backed by an array. The array must be sorted, and is
     * not copied.
     *
     * @param values Array of values
     */
    public ArraySortedSet(E[] values) {
        this(values, 0, values.length);
    }

    /**
     * Creates a set backed by a region of an array. The array must be
     * sorted, and is not copied.
     *
     * @param values Array of values
     * @param start Index of start of region
     * @param end Index of first element after end of region
     */
    public ArraySortedSet(E[] values, int start, int end) {
        this.values = values;
        this.start = start;
        this.end = end;
    }

    public Iterator<E> iterator() {
        return asList().iterator();
    }

    public int size() {
        return end - start;
    }

    public Comparator<? super E> comparator() {
        return null;
    }

    public SortedSet<E> subSet(E fromElement, E toElement) {
        int from = Util.binarySearch(values, start, end, fromElement);
        if (from < 0) {
            from = - (from + 1);
        }
        int to = Util.binarySearch(values, from, end, toElement);
        if (to < 0) {
            to = - (to + 1);
        }
        return subset(from, to);
    }

    public SortedSet<E> headSet(E toElement) {
        int to = Util.binarySearch(values, start, end, toElement);
        if (to < 0) {
            to = - (to + 1);
        }
        return subset(start, to);
    }

    public SortedSet<E> tailSet(E fromElement) {
        int from = Util.binarySearch(values, start, end, fromElement);
        if (from < 0) {
            from = - (from + 1);
        }
        return subset(from, end);
    }

    private SortedSet<E> subset(int from, int to) {
        if (from == start && to == end) {
            return this;
        }
        return new ArraySortedSet<E>(values, from, to);
    }

    private List<E> asList() {
        //noinspection unchecked
        List<E> list = Arrays.asList(values);
        if (start > 0 || end < values.length) {
            list = list.subList(start, end);
        }
        return list;
    }

    public E first() {
        try {
            return values[start];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }

    public E last() {
        try {
            return values[end - 1];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Object[] toArray() {
        if (start == 0 && end == values.length) {
            return values.clone();
        } else {
            final Object[] os = new Object[end - start];
            System.arraycopy(values, start, os, 0, end - start);
            return os;
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T[] toArray(T[] a) {
        int size = size();
        T[] r = a.length >= size
            ? a
            : (T[]) java.lang.reflect.Array.newInstance(
                a.getClass().getComponentType(), size);
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(values, start, r, 0, end - start);
        if (r.length > size) {
            r[size] = null;
        }
        return r;
    }

    @Override
    public boolean contains(Object o) {
        //noinspection unchecked
        return o != null
            && Util.binarySearch(values, start, end, (E) o) >= 0;
    }
}

// End ArraySortedSet.java
