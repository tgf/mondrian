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

import mondrian.olap.Util;
import mondrian.spi.*;
import mondrian.util.CompletedFuture;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Mock implementation of {@link SegmentCache} that is used for automated
 * testing.
 *
 * <P>It tries to marshall / unmarshall all {@link SegmentHeader} and
 * {@link SegmentBody} objects that are sent to it.
 *
 * @author LBoudreau
 * @version $Id$
 */
public class MockSegmentCache implements SegmentCache {
    private static final Map<SegmentHeader, SegmentBody> cache =
        new ConcurrentHashMap<SegmentHeader, SegmentBody>();

    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<SegmentCacheListener>();

    private final static int maxElements = 100;

    public Future<Boolean> contains(SegmentHeader header) {
        return new CompletedFuture<Boolean>(
            cache.containsKey(header), null);
    }

    public Future<SegmentBody> get(SegmentHeader header) {
        return new CompletedFuture<SegmentBody>(
            cache.get(header), null);
    }

    public Future<Boolean> put(
        final SegmentHeader header,
        final SegmentBody body)
    {
        // Try to serialize back and forth. if the tests fail because of this,
        // then the objects could not be serialized properly.
        // First try with the header
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(header);
            oos.close();
            // deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            SegmentHeader o = (SegmentHeader) ois.readObject();
            Util.discard(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Now try it with the body.
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(body);
            oos.close();
            // deserialize
            byte[] pickled = out.toByteArray();
            InputStream in = new ByteArrayInputStream(pickled);
            ObjectInputStream ois = new ObjectInputStream(in);
            SegmentBody o = (SegmentBody) ois.readObject();
            Util.discard(o);
        } catch (NotSerializableException e) {
            throw new RuntimeException(
                "while serializing " + body,
                e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cache.put(header, body);
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener
                .SegmentCacheEvent()
            {
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
        if (cache.size() > maxElements) {
            // Cache is full. pop one out at random.
            final double index =
                Math.floor(maxElements * Math.random());
            cache.remove(index);
            fireSegmentCacheEvent(
                new SegmentCache.SegmentCacheListener
                    .SegmentCacheEvent()
                {
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
        return new CompletedFuture<Boolean>(true, null);
    }

    public Future<List<SegmentHeader>> getSegmentHeaders() {
        return new CompletedFuture<List<SegmentHeader>>(
            new ArrayList<SegmentHeader>(cache.keySet()), null);
    }

    public Future<Boolean> remove(final SegmentHeader header) {
        cache.remove(header);
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener
                .SegmentCacheEvent()
            {
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
        return new CompletedFuture<Boolean>(true, null);
    }

    public void tearDown() {
        listeners.clear();
        cache.clear();
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

// End MockSegmentCache.java
