/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.*;

/**
 * Implementation of a segment body which stores the data inside
 * a dense primitive array of integers.
 *
 * @author LBoudreau
 * @version $Id$
 */
class DenseIntSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = 5391233622968115488L;

    private final int[] values;
    private final BitSet nullIndicators;

    /**
     * Creates a DenseIntSegmentBody.
     *
     * <p>Stores the given array of cell values and null indicators; caller must
     * not modify them afterwards.</p>
     *
     * @param nullIndicators Null indicators
     * @param values Cell values
     * @param axes Axes
     */
    DenseIntSegmentBody(
        BitSet nullIndicators,
        int[] values,
        List<Pair<SortedSet<Comparable<?>>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
        this.nullIndicators = nullIndicators;
    }

    public SegmentDataset createSegmentDataset(
        Segment segment,
        SegmentAxis[] axes)
    {
        return new DenseIntSegmentDataset(axes, values, nullIndicators);
    }
}

// End DenseIntSegmentBody.java
