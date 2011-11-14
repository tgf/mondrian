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

import mondrian.rolap.BitKey;
import mondrian.rolap.agg.SegmentHeader;
import mondrian.util.ByteString;
import mondrian.util.Pair;

import java.util.List;
import java.util.Map;

/**
 * Data structure that identifies which segments contain cells.
 *
 * <p>Not thread safe.</p>
 *
 * @version $Id$
 * @author Julian Hyde
 */
public interface SegmentCacheIndex {
    /**
     * Identifies the segment headers that contain a given cell.
     */
    List<SegmentHeader> locate(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords);

    /**
     * Adds a header to the index.
     */
    void add(
        SegmentHeader header);
}

// End SegmentCacheIndex.java
