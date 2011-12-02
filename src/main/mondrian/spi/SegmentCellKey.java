/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.rolap.BitKey;
import mondrian.rolap.agg.SegmentBuilder;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A SegmentCellKey is an essentially the same as a CellKey,
 * but in an extremely lightweight form. It is represented
 * as an array of integers, describing a cell position.
 *
 * <p>Light Cell Keys are used extremely frequently by
 * {@link SegmentBuilder#rollup(java.util.Map, java.util.Set,
 * BitKey, mondrian.olap.Aggregator)} so they need to remain as
 * lightweight and cheap as possible.
 *
 * <p>A light cell key is to be considered immutable. It precomputes
 * its hash code for performance reasons, so modifying its coordinates
 * will have unknown consequences.
 *
 * <p>It is also a top level class. This prevents it from having a reference
 * to its parent class and makes it easier for the GC to collect them.
 *
 * @author LBoudreau
 * @version $Id$
 */
public class SegmentCellKey implements Serializable {
    private static final long serialVersionUID = -2508334535841993876L;
    private final int[] pos;
    private final int hash;
    public SegmentCellKey(int[] pos) {
        this.pos = Arrays.copyOf(pos, pos.length);
        int h = 17;
        for (int ordinal : this.pos) {
            h = Util.hash(h, ordinal);
        }
        this.hash = h;
    }
    public int hashCode() {
        return this.hash;
    }
    public boolean equals(Object obj) {
        if (obj instanceof SegmentCellKey) {
            return Arrays.equals(this.pos, ((SegmentCellKey)obj).pos);
        }
        return false;
    }
    public int[] getOrdinals() {
        return pos;
    }
    public int getArrity() {
        return pos.length;
    }
}
// End SegmentCellKey.java