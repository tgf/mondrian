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
import mondrian.spi.SegmentColumn;
import mondrian.spi.SegmentHeader;
import mondrian.util.ByteString;

import java.io.PrintWriter;
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
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates);

    /**
     * Returns a list of segments that can be rolled up to satisfy a given
     * cell request.
     */
    List<List<SegmentHeader>> findRollupCandidates(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable<?>> coords,
        String[] compoundPredicates);

    public List<SegmentHeader> intersectRegion(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        SegmentColumn[] region);

    /**
     * Adds a header to the index.
     */
    void add(SegmentHeader header);

    /**
     * Removes a header from the index.
     */
    void remove(SegmentHeader header);

    void printCacheState(PrintWriter pw);
}

// End SegmentCacheIndex.java
