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

import mondrian.rolap.agg.SegmentBody;
import mondrian.rolap.agg.SegmentHeader;
import mondrian.spi.SegmentCache;
import mondrian.util.CompletedFuture;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * Implementation of {@link mondrian.spi.SegmentCache} that stores segments
 * in memory.
 *
 * <p>Segments are held via soft references, so the garbage collector can remove
 * them if it sees fit.</p>
 *
 * <p>Not thread safe.</p>
 *
 * @version $Id$
 * @author Julian Hyde
 */
public class MemorySegmentCache implements SegmentCache {
    private final Map<SegmentHeader, SoftReference<SegmentBody>> map =
        new HashMap<SegmentHeader, SoftReference<SegmentBody>>();

    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<SegmentCacheListener>();

    public Future<SegmentBody> get(SegmentHeader header) {
        try {
            final SoftReference<SegmentBody> ref = map.get(header);
            if (ref == null) {
                return CompletedFuture.success(null);
            }
            final SegmentBody body = ref.get();
            if (body == null) {
                map.remove(header);
            }
            return CompletedFuture.success(body);
        } catch (Throwable e) {
            return CompletedFuture.fail(e);
        }
    }

    public Future<Boolean> contains(SegmentHeader header) {
        try {
            final SoftReference<SegmentBody> ref = map.get(header);
            if (ref == null) {
                return CompletedFuture.success(Boolean.FALSE);
            }
            final SegmentBody body = ref.get();
            if (body == null) {
                map.remove(header);
                return CompletedFuture.success(Boolean.FALSE);
            }
            return CompletedFuture.success(Boolean.TRUE);
        } catch (Throwable e) {
            return CompletedFuture.fail(e);
        }
    }

    public Future<List<SegmentHeader>> getSegmentHeaders() {
        try {
            final List<SegmentHeader> list =
                new ArrayList<SegmentHeader>(map.keySet());
            return CompletedFuture.success(list);
        } catch (Throwable e) {
            return CompletedFuture.fail(e);
        }
    }

    public Future<Boolean> put(final SegmentHeader header, SegmentBody body) {
        // REVIEW: What's the difference between returning Future(false)
        // and returning Future(exception)?
        try {
            map.put(header, new SoftReference<SegmentBody>(body));
            fireSegmentCacheEvent(
                new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                    public boolean isLocal() {
                        return true;
                    }
                    public SegmentHeader getSource() {
                        return header;
                    }
                    public EventType getEventType() {
                        return
                            SegmentCacheListener.SegmentCacheEvent
                                .EventType.ENTRY_CREATED;
                    }
                });
            return CompletedFuture.success(Boolean.TRUE);
        } catch (Throwable e) {
            return CompletedFuture.fail(e);
        }
    }

    public Future<Boolean> remove(final SegmentHeader header) {
        try {
            final boolean result =
                map.remove(header) != null;
            if (result) {
                fireSegmentCacheEvent(
                    new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                        public boolean isLocal() {
                            return true;
                        }
                        public SegmentHeader getSource() {
                            return header;
                        }
                        public EventType getEventType() {
                            return
                                SegmentCacheListener.SegmentCacheEvent
                                    .EventType.ENTRY_DELETED;
                        }
                    });
            }
            return CompletedFuture.success(result);
        } catch (Throwable e) {
            return CompletedFuture.fail(e);
        }
    }

    public void tearDown() {
        map.clear();
    }

    public void addListener(SegmentCacheListener l) {
        listeners.add(l);
    }

    public void removeListener(SegmentCacheListener l) {
        listeners.remove(l);
    }

    public boolean supportsRichIndex() {
        return true;
    }

    public void fireSegmentCacheEvent(
        SegmentCache.SegmentCacheListener.SegmentCacheEvent evt)
    {
        for (SegmentCacheListener l : listeners) {
            l.handle(evt);
        }
    }
}

// End MemorySegmentCache.java
