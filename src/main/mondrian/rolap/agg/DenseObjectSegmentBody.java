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

import java.util.List;
import java.util.SortedSet;

/**
 * Implementation of a segment body which stores the data inside
 * a dense array of Java objects.
 *
 * @author LBoudreau
 * @version $Id$
 */
class DenseObjectSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -3558427982849392173L;

    private final Object[] values;

    /**
     * Creates a DenseObjectSegmentBody.
     *
     * <p>Stores the given array of cell values; caller must not modify it
     * afterwards.</p>
     *
     * @param values Cell values
     * @param axes Axes
     */
    DenseObjectSegmentBody(
        Object[] values,
        List<Pair<SortedSet<Comparable<?>>, Boolean>> axes)
    {
        super(axes);
        this.values = values;
    }

    public SegmentDataset createSegmentDataset(
        Segment segment,
        SegmentAxis[] axes)
    {
        DenseObjectSegmentDataset ds =
            new DenseObjectSegmentDataset(axes, values);
        System.arraycopy(values, 0, ds.values, 0, values.length);
        return ds;
    }
}

// End DenseObjectSegmentBody.java
