/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.List;
import java.util.SortedSet;

/**
 * Abstract implementation of a SegmentBody.
 *
 * @author LBoudreau
 * @version $Id$
 */
abstract class AbstractSegmentBody implements SegmentBody {
    private static final long serialVersionUID = -7094121704771005640L;

    private final SortedSet<Comparable<?>>[] axisValueSets;
    private final boolean[] nullAxisFlags;

    public AbstractSegmentBody(
        List<Pair<SortedSet<Comparable<?>>, Boolean>> axes)
    {
        super();
        //noinspection unchecked
        this.axisValueSets = new SortedSet[axes.size()];
        this.nullAxisFlags = new boolean[axes.size()];
        for (int i = 0; i < axes.size(); i++) {
            Pair<SortedSet<Comparable<?>>, Boolean> pair = axes.get(i);
            axisValueSets[i] = pair.left;
            nullAxisFlags[i] = pair.right;
        }
    }

    public SortedSet<Comparable<?>>[] getAxisValueSets() {
        return axisValueSets;
    }

    public boolean[] getNullAxisFlags() {
        return nullAxisFlags;
    }
}

// End AbstractSegmentBody.java
