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

import mondrian.rolap.CellKey;

import java.util.Map;
import java.util.SortedSet;

/**
 * Implementation of a segment body which stores the data of a
 * sparse segment data set into a dense array of java objects.
 *
 * @author LBoudreau
 * @version $Id$
 */
class SparseSegmentBody extends AbstractSegmentBody {
    private static final long serialVersionUID = -6684830985364895836L;
    final CellKey[] keys;
    final Object[] data;

    SparseSegmentBody(
        Map<CellKey, Object> dataToSave,
        SortedSet<Comparable<?>>[] axisValueSets,
        boolean[] nullAxisFlags)
    {
        super(axisValueSets, nullAxisFlags);

        this.keys = new CellKey[dataToSave.size()];
        this.data = new Object[dataToSave.size()];
        int i = 0;
        for (Map.Entry<CellKey, Object> entry : dataToSave.entrySet()) {
            keys[i] = entry.getKey();
            data[i] = entry.getValue();
            ++i;
        }
    }

    public SegmentDataset createSegmentDataset(
        Segment segment,
        SegmentAxis[] axes)
    {
        SparseSegmentDataset ds =
            new SparseSegmentDataset();
        for (int i = 0; i < keys.length; i++) {
            ds.put(this.keys[i], this.data[i]);
        }
        return ds;
    }
}

// End SparseSegmentBody.java
