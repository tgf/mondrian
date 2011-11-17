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
import mondrian.rolap.agg.SegmentHeader;
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

    /**
     * Creates a SegmentCacheIndexImpl.
     */
    public SegmentCacheIndexImpl() {
    }

    public List<SegmentHeader> locate(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords)
    {
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
                    coords))
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
        headerList.add(header);
    }

    private boolean matches(
        SegmentHeader header,
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords)
    {
        // most selective condition first
        if (!header.constrainedColsBitKey.equals(constrainedColsBitKey)) {
            return false;
        }
        if (!header.schemaName.equals(schemaName)) {
            return false;
        }
        // REVIEW: make schemaChecksum mandatory?
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
        for (Map.Entry<String, Comparable<?>> entry : coords.entrySet()) {
            final SegmentHeader.ConstrainedColumn constrainedColumn =
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
}

// End SegmentCacheIndexImpl.java
