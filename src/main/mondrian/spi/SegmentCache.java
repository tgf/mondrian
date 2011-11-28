/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.spi;

import mondrian.olap.MondrianProperties;
import mondrian.spi.SegmentCache.SegmentCacheListener.SegmentCacheEvent;
import mondrian.spi.SegmentHeader;

import java.util.List;
import java.util.concurrent.Future;

/**
 * SPI definition of the segments cache.
 *
 * <p>Lookups are performed using {@link SegmentHeader}s and
 * {@link SegmentBody}s. Both are immutable and fully serializable.
 *
 * <p>There are two ways to declare a SegmentCache implementation in
 * Mondrian. The first one (and the one which will be used by default)
 * is to set the {@link MondrianProperties#SegmentCache} property. The
 * second one is to use the Java Services API. You will need to create
 * a jar file, accessible through the same class loader as Mondrian,
 * and add a file called <code>/META-INF/services/mondrian.spi.SegmentCache
 * </code> which contains the name of the segment cache implementation
 * to use. If more than one SegmentCache Java service is found, the first
 * one found is used. This is a non-deterministic choice as there are
 * no guarantees as to which will appear first. This later mean of discovery
 * is overridden by defining the {@link MondrianProperties#SegmentCache}
 * property.
 *
 * <p>Implementations are expected to be thread-safe.
 * It is the responsibility of the cache implementation
 * to maintain a consistent state.
 *
 * <p>Implementations must provide a default empty constructor.
 * Mondrian creates one segment cache instance per Mondrian server.
 * There could be more than one Mondrian server running in the same JVM.
 *
 * @see MondrianProperties#SegmentCache
 * @author LBoudreau
 * @version $Id$
 */
public interface SegmentCache {
    /**
     * Returns a future SegmentBody object once the
     * cache has returned any results, or null of no
     * segment corresponding to the header could be found.
     * @param header The header of the segment to find.
     * Consider this as a key.
     * @return A Future SegmentBody or a Future <code>null</code>
     * if no corresponding segment could be found in cache.
     */
    Future<SegmentBody> get(SegmentHeader header);

    /**
     * Checks if the cache contains a {@link SegmentBody} corresponding
     * to the supplied {@link SegmentHeader}.
     * @param header A header to lookup in the cache.
     * @return A Future true or a Future false
     * if no corresponding segment could be found in cache.
     */
    Future<Boolean> contains(SegmentHeader header);

    /**
     * Returns a list of all segments present in the cache.
     * @return A List of segment headers describing the
     * contents of the cache.
     */
    Future<List<SegmentHeader>> getSegmentHeaders();

    /**
     * Stores a segment data in the cache.
     * @return A Future object which returns true or false
     * depending on the success of the caching operation.
     * @param header The header of the segment.
     * @param body The segment body to cache.
     */
    Future<Boolean> put(SegmentHeader header, SegmentBody body);

    /**
     * Removes a segment from the cache.
     * @param header The header of the segment we want to remove.
     * @return True if the segment was found and removed,
     * false otherwise.
     */
    Future<Boolean> remove(SegmentHeader header);

    /**
     * Tear down and clean up the cache.
     */
    void tearDown();

    /**
     * Adds a listener to this segment cache implementation.
     * The listener will get notified via {@link SegmentCacheEvent}
     * instances.
     * @param l The listener to attach to this cache.
     */
    void addListener(SegmentCacheListener l);

    /**
     * Unregisters a listener from this segment cache implementation.
     * @param l The listener to remove.
     */
    void removeListener(SegmentCacheListener l);

    /**
     * Tells Mondrian whether this segment cache uses the {@link SegmentHeader}
     * objects as an index, thus preserving them in a serialized state, or if
     * it uses its identification number only. Not using a rich index prevents
     * Mondrian from doing partial cache invalidation.
     */
    boolean supportsRichIndex();

    /**
     * {@link SegmentCacheListener} objects are used to listen
     * to the state of the cache and be notified of changes to its
     * state or its entries. Mondrian will automatically register
     * a listener with the implementations it uses.
     *
     * Implementations of SegmentCache should only send events if the
     * cause of the event is not Mondrian itself. Only in cases where
     * the cache gets updated by other Mondrian nodes or by a third
     * party application is it required to use this interface.
     */
    interface SegmentCacheListener {
        /**
         * Handle an event
         * @param e Event to handle.
         */
        void handle(SegmentCacheEvent e);

        /**
         * Defines the event types that a listener can look for.
         */
        interface SegmentCacheEvent {
            /**
             * Defined the possible types of events used by
             * the {@link SegmentCacheListener} class.
             */
            enum EventType {
                /**
                 * An Entry was created in cache.
                 */
                ENTRY_CREATED,
                /**
                 * An entry was deleted from the cache.
                 */
                ENTRY_DELETED
            }

            /**
             * Returns the event type of the current SegmentCacheEvent
             * instance.
             */
            EventType getEventType();

            /**
             * Returns the segment header at the source of the event.
             */
            SegmentHeader getSource();

            /**
             * Tells whether or not this event was a local event or
             * an event triggered by an operation on a remote node.
             * If the implementation cannot differentiate or doesn't
             * support remote nodes, always return false.
             */
            boolean isLocal();
        }
    }
}
// End SegmentCache.java
