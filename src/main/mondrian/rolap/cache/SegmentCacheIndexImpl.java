/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import mondrian.olap.Util;
import mondrian.rolap.BitKey;
import mondrian.spi.SegmentHeader;
import mondrian.spi.ConstrainedColumn;
import mondrian.util.ByteString;

import java.util.*;

/**
 * Data structure that identifies which segments contain cells.
 *
 * <p>Not thread safe.</p>
 *
 * @version $Id$
 * @author Julian Hyde
 */
public class SegmentCacheIndexImpl implements SegmentCacheIndex {
    private final List<SegmentHeader> headerList =
        new ArrayList<SegmentHeader>();
    private final Thread thread;

    /**
     * Creates a SegmentCacheIndexImpl.
     *
     * @param thread Thread that must be used to execute commands.
     */
    public SegmentCacheIndexImpl(Thread thread) {
        this.thread = thread;
    }

    public List<SegmentHeader> locate(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates)
    {
        assert thread == Thread.currentThread()
            : "expected " + thread + ", but was " + Thread.currentThread();
        List<SegmentHeader> list = Collections.emptyList();
        for (SegmentHeader header : headerList) {
            if (matches(
                    header,
                    schemaName,
                    schemaChecksum,
                    cubeName,
                    measureName,
                    rolapStarFactTableName,
                    constrainedColsBitKey,
                    coords,
                    compoundPredicates))
            {
                // Be lazy. Don't allocate a list unless there is at least one
                // entry.
                if (list.isEmpty()) {
                    list = new ArrayList<SegmentHeader>();
                }
                list.add(header);
            }
        }
        return list;
    }

    public void add(SegmentHeader header) {
        assert thread == Thread.currentThread()
            : "expected " + thread + ", but was " + Thread.currentThread();
        headerList.add(header);
    }

    public void remove(SegmentHeader header) {
        headerList.remove(header);
    }

    private boolean matches(
        SegmentHeader header,
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates)
    {
        // most selective condition first
        if (!header.getConstrainedColumnsBitKey()
            .equals(constrainedColsBitKey))
        {
            return false;
        }
        if (!header.schemaName.equals(schemaName)) {
            return false;
        }
        if (!Util.equals(header.schemaChecksum, schemaChecksum)) {
            return false;
        }
        if (!header.cubeName.equals(cubeName)) {
            return false;
        }
        if (!header.measureName.equals(measureName)) {
            return false;
        }
        if (!header.rolapStarFactTableName.equals(rolapStarFactTableName)) {
            return false;
        }
        if (!Arrays.equals(header.compoundPredicates, compoundPredicates)) {
            return false;
        }
        for (Map.Entry<String, Comparable<?>> entry : coords.entrySet()) {
            // Check if the segment explicitly excludes this coordinate.
            final ConstrainedColumn excludedColumn =
                header.getExcludedRegion(entry.getKey());
            if (excludedColumn != null) {
                final Object[] values = excludedColumn.getValues();
                if (values == null
                    || Arrays.asList(values).contains(entry.getValue()))
                {
                    return false;
                }
            }
            // Check if the dimensionality of the segment intersects
            // with the coordinate.
            final ConstrainedColumn constrainedColumn =
                header.getConstrainedColumn(entry.getKey());
            if (constrainedColumn == null) {
                throw Util.newInternal(
                    "Segment axis for column '"
                    + entry.getKey()
                    + "' not found");
            }
            final Object[] values = constrainedColumn.getValues();
            if (values != null
                && !Arrays.asList(values).contains(entry.getValue()))
            {
                return false;
            }
        }
        return true;
    }

    public List<SegmentHeader> intersectRegion(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        ConstrainedColumn[] region)
    {
        assert thread == Thread.currentThread()
            : "expected " + thread + ", but was " + Thread.currentThread();
        List<SegmentHeader> list = Collections.emptyList();
        for (SegmentHeader header : headerList) {
            if (intersects(
                    header,
                    schemaName,
                    schemaChecksum,
                    cubeName,
                    measureName,
                    rolapStarFactTableName,
                    region))
            {
                // Be lazy. Don't allocate a list unless there is at least one
                // entry.
                if (list.isEmpty()) {
                    list = new ArrayList<SegmentHeader>();
                }
                list.add(header);
            }
        }
        return list;
    }

    private boolean intersects(
        SegmentHeader header,
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        ConstrainedColumn[] region)
    {
        // most selective condition first
        if (!header.schemaName.equals(schemaName)) {
            return false;
        }
        if (!Util.equals(header.schemaChecksum, schemaChecksum)) {
            return false;
        }
        if (!header.cubeName.equals(cubeName)) {
            return false;
        }
        if (!header.measureName.equals(measureName)) {
            return false;
        }
        if (!header.rolapStarFactTableName.equals(rolapStarFactTableName)) {
            return false;
        }
        if (region.length == 0) {
            return true;
        }
        for (ConstrainedColumn regionColumn : region) {
            final ConstrainedColumn headerColumn =
                header.getConstrainedColumn(regionColumn.getColumnExpression());
            if (headerColumn == null) {
                /*
                 * If the segment header doesn't contain a column specified
                 * by the region, then it always implicitly intersects.
                 * This allows flush operations to be valid.
                 */
                return true;
            }
            final Object[] regionValues = regionColumn.getValues();
            final Object[] headerValues = regionColumn.getValues();
            if (headerValues == null || regionValues == null) {
                /*
                 * This is a wildcard, so it always intersects.
                 */
                return true;
            }
            final Set<Object> headerValuesHashedSet =
                new HashSet<Object>(Arrays.asList(headerValues));
            for (Object myValue : regionValues) {
                if (headerValuesHashedSet.contains(myValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<SegmentHeader> getAllHeaders() {
        return Collections.unmodifiableList(headerList);
    }
}

// End SegmentCacheIndexImpl.java
