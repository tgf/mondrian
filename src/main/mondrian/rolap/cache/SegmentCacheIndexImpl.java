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
import mondrian.rolap.RolapUtil;
import mondrian.spi.SegmentColumn;
import mondrian.spi.SegmentHeader;
import mondrian.util.ByteString;
import mondrian.util.PartiallyOrderedSet;

import java.io.PrintWriter;
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
    private final Map<List, FactInfo> factMap =
        new HashMap<List, FactInfo>();

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
        FactInfo factInfo = factMap.get(factKey);
        if (factInfo == null) {
            factInfo = new FactInfo();
            factMap.put(factKey, factInfo);
        }
        factInfo.headerList.add(header);
        factInfo.bitkeyPoset.add(header.getConstrainedColumnsBitKey());
    }

    public void remove(SegmentHeader header) {
        final List factKey = makeFactKey(header);
        final FactInfo factInfo = factMap.get(factKey);
        factInfo.headerList.remove(header);
        if (factInfo.headerList.size() == 0) {
            factMap.remove(factKey);
        }

        final List bitkeyKey = makeBitkeyKey(header);
        final List<SegmentHeader> headerList = bitkeyMap.get(bitkeyKey);
        headerList.remove(header);
        if (headerList.size() == 0) {
            bitkeyMap.remove(bitkeyKey);
            factInfo.bitkeyPoset.remove(header.getConstrainedColumnsBitKey());
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
        final FactInfo factInfo = factMap.get(factKey);
        List<SegmentHeader> list = Collections.emptyList();
        if (factInfo == null) {
            return list;
        }
        for (SegmentHeader header : factInfo.headerList) {
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
                // If the segment header doesn't contain a column specified
                // by the region, then it always implicitly intersects.
                // This allows flush operations to be valid.
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

    public void printCacheState(PrintWriter pw) {
        for (List<SegmentHeader> headerList : bitkeyMap.values()) {
            for (SegmentHeader header : headerList) {
                pw.println(header.getDescription());
            }
        }
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

    public List<List<SegmentHeader>> findRollupCandidates(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates)
    {
        final List factKey = makeFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName);
        final FactInfo factInfo = factMap.get(factKey);
        List<List<SegmentHeader>> list = Collections.emptyList();
        if (factInfo == null) {
            return list;
        }

        // Iterate over all dimensionalities that are a superset of the desired
        // columns and for which a segment is known to exist.
        //
        // It helps that getAncestors returns dimensionalities with fewer bits
        // set first. These will contain fewer cells, and therefore be less
        // effort to roll up.
        final List<SegmentHeader> matchingHeaders =
            new ArrayList<SegmentHeader>();

        final List<BitKey> ancestors =
            factInfo.bitkeyPoset.getAncestors(constrainedColsBitKey);
        for (BitKey bitKey : ancestors) {
            final List bitkeyKey = makeBitkeyKey(
                schemaName,
                schemaChecksum,
                cubeName,
                rolapStarFactTableName,
                bitKey,
                measureName);
            final List<SegmentHeader> headers = bitkeyMap.get(bitkeyKey);
            assert headers != null : "bitkeyPoset / bitkeyMap inconsistency";

            // For columns that are still present after roll up, make sure that
            // the required value is in the range covered by the segment.
            // Of the columns that are being aggregated away, are all of
            // them wildcarded? If so, this segment is a match. If not, we
            // will need to combine with other segments later.
            matchingHeaders.clear();
            headerLoop:
            for (SegmentHeader header : headers) {
                int nonWildcardCount = 0;
                for (SegmentColumn column : header.getConstrainedColumns()) {
                    final SegmentColumn constrainedColumn =
                        header.getConstrainedColumn(
                            column.columnExpression);

                    // REVIEW: How are null key values represented in coords?
                    // Assuming that they are represented by null ref.
                    if (coords.containsKey(column.columnExpression)) {
                        // Matching column. Will not be aggregated away. Needs
                        // to be in range.
                        Comparable<?> value =
                            coords.get(column.columnExpression);
                        if (value == null) {
                            value = RolapUtil.sqlNullValue;
                        }
                        if (constrainedColumn.values != null
                            && !constrainedColumn.values.contains(value))
                        {
                            continue headerLoop;
                        }
                    } else {
                        // Non-matching column. Will be aggregated away. Needs
                        // to be wildcarded (or some more complicated conditions
                        // to be dealt with later).
                        if (constrainedColumn.values != null) {
                            ++nonWildcardCount;
                        }
                    }
                }

                if (nonWildcardCount == 0) {
                    if (list.isEmpty()) {
                        list = new ArrayList<List<SegmentHeader>>();
                    }
                    list.add(Collections.singletonList(header));
                } else {
                    matchingHeaders.add(header);
                }
            }

            // TODO: Find combinations of segments that can roll up. Not
            // possible if matchingHeaders is empty.
            Util.discard(matchingHeaders);
        }
        return list;
    }


    private static class FactInfo {
        private static final PartiallyOrderedSet.Ordering<BitKey> ORDERING =
            new PartiallyOrderedSet.Ordering<BitKey>() {
                public boolean lessThan(BitKey e1, BitKey e2) {
                    return e2.isSuperSetOf(e1);
                }
            };

        private final List<SegmentHeader> headerList =
            new ArrayList<SegmentHeader>();

        private final PartiallyOrderedSet<BitKey> bitkeyPoset =
            new PartiallyOrderedSet<BitKey>(ORDERING);

        FactInfo() {
        }
    }
}

// End SegmentCacheIndexImpl.java
