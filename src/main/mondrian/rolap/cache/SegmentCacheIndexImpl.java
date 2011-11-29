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
import mondrian.spi.SegmentColumn;
import mondrian.spi.SegmentHeader;
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
    private final Map<List, List<SegmentHeader>> bitkeyMap =
        new HashMap<List, List<SegmentHeader>>();
    private final Map<List, List<SegmentHeader>> factMap =
        new HashMap<List, List<SegmentHeader>>();

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
        final List starKey =
            makeBitkeyKey(
                schemaName,
                schemaChecksum,
                cubeName,
                rolapStarFactTableName,
                constrainedColsBitKey,
                measureName);
        final List<SegmentHeader> headerList = bitkeyMap.get(starKey);
        if (headerList == null) {
            return Collections.emptyList();
        }
        for (SegmentHeader header : headerList) {
            if (matches(header, coords, compoundPredicates)) {
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
        final List bitkeyKey = makeBitkeyKey(header);
        List<SegmentHeader> headerList = bitkeyMap.get(bitkeyKey);
        if (headerList == null) {
            headerList = new ArrayList<SegmentHeader>();
            bitkeyMap.put(bitkeyKey, headerList);
        }
        headerList.add(header);

        final List factKey = makeFactKey(header);
        List<SegmentHeader> headerList2 = bitkeyMap.get(factKey);
        if (headerList2 == null) {
            headerList2 = new ArrayList<SegmentHeader>();
            factMap.put(factKey, headerList2);
        }
        headerList2.add(header);
    }

    public void remove(SegmentHeader header) {
        final List bitkeyKey = makeBitkeyKey(header);
        final List<SegmentHeader> headerList = bitkeyMap.get(bitkeyKey);
        if (headerList != null) {
            headerList.remove(header);
        }
    }

    private boolean matches(
        SegmentHeader header,
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates)
    {
        if (!Arrays.equals(header.compoundPredicates, compoundPredicates)) {
            return false;
        }
        for (Map.Entry<String, Comparable<?>> entry : coords.entrySet()) {
            // Check if the segment explicitly excludes this coordinate.
            final SegmentColumn excludedColumn =
                header.getExcludedRegion(entry.getKey());
            if (excludedColumn != null) {
                final SortedSet<Comparable<?>> values =
                    excludedColumn.getValues();
                if (values == null || values.contains(entry.getValue())) {
                    return false;
                }
            }
            // Check if the dimensionality of the segment intersects
            // with the coordinate.
            final SegmentColumn constrainedColumn =
                header.getConstrainedColumn(entry.getKey());
            if (constrainedColumn == null) {
                throw Util.newInternal(
                    "Segment axis for column '"
                    + entry.getKey()
                    + "' not found");
            }
            final SortedSet<Comparable<?>> values =
                constrainedColumn.getValues();
            if (values != null
                && !values.contains(entry.getValue()))
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
        SegmentColumn[] region)
    {
        assert thread == Thread.currentThread()
            : "expected " + thread + ", but was " + Thread.currentThread();
        final List factKey = makeFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName);
        final List<SegmentHeader> headerList = factMap.get(factKey);
        if (headerList == null) {
            return Collections.emptyList();
        }
        List<SegmentHeader> list = Collections.emptyList();
        for (SegmentHeader header : headerList) {
            if (intersects(header, region)) {
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
        SegmentColumn[] region)
    {
        // most selective condition first
        if (region.length == 0) {
            return true;
        }
        for (SegmentColumn regionColumn : region) {
            final SegmentColumn headerColumn =
                header.getConstrainedColumn(regionColumn.getColumnExpression());
            if (headerColumn == null) {
                /*
                 * If the segment header doesn't contain a column specified
                 * by the region, then it always implicitly intersects.
                 * This allows flush operations to be valid.
                 */
                return true;
            }
            final SortedSet<Comparable<?>> regionValues =
                regionColumn.getValues();
            final SortedSet<Comparable<?>> headerValues =
                regionColumn.getValues();
            if (headerValues == null || regionValues == null) {
                // This is a wildcard, so it always intersects.
                return true;
            }
            for (Comparable<?> myValue : regionValues) {
                if (headerValues.contains(myValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<SegmentHeader> getAllHeaders() {
        return Collections.emptyList();
    }

    private List makeBitkeyKey(SegmentHeader header) {
        return makeBitkeyKey(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.constrainedColsBitKey,
            header.measureName);
    }

    private List makeBitkeyKey(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        String measureName)
    {
        return Arrays.asList(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            constrainedColsBitKey,
            measureName);
    }

    private List makeFactKey(SegmentHeader header) {
        return makeFactKey(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.measureName);
    }

    private List makeFactKey(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName)
    {
        return Arrays.asList(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName);
    }
}

// End SegmentCacheIndexImpl.java
