/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.cache.SegmentCacheIndex;
import mondrian.rolap.cache.SegmentCacheIndexImpl;
import mondrian.util.Pair;

import java.util.*;
import java.util.concurrent.*;

/**
 * Active object that maintains the "global cache" (in JVM, but shared between
 * connections using a particular schema) and "external cache" (as implemented
 * by a {@link mondrian.spi.SegmentCache}.
 *
 * <p>Segment states</p>
 *
 * <table>
 *     <tr><th>State</th><th>Meaning</th></tr>
 *     <tr><td>Local</td><td>Initial state of a segment</td></tr>
 * </table>
 *
 * <h2>Decisions to be reviewed</h2>
 *
 * <p>1. Create variant of actor that processes all requests synchronously,
 * and does not need a thread. This would be a more 'embedded' mode of operation
 * (albeit with worse scale-out).</p>
 *
 * <p>2. Move functionality into AggregationManager?</p>
 *
 * <p>3. Delete {@link mondrian.rolap.RolapStar#lookupOrCreateAggregation}
 * and {@link mondrian.rolap.RolapStar#lookupSegment}
 * and {@link mondrian.rolap.RolapStar}.lookupAggregationShared
 * (formerly RolapStar.lookupAggregation).</p>
 *
 * <p>4. {@link Aggregation#flush}
 * is now static, can be moved somewhere else</p>
 *
 *
 *
 * <h2>Moved methods</h2>
 *
 * <p>(Keeping track of where methods came from will make it easier to merge
 * to the mondrian-4 code line.)</p>
 *
 * <p>1. {@link mondrian.rolap.RolapStar#getCellFromCache} moved from
 * {@link Aggregation}.getCellValue</p>
 *
 *
 *
 * <h2>Checked in</h3>
 *
 * <p>1. Obsolete CountingAggregationManager, and property
 * mondrian.rolap.agg.enableCacheHitCounters.</p>
 *
 * <p>2. AggregationManager becomes non-singleton.</p>
 *
 * <p>3. SegmentCacheWorker methods and segmentCache field become
 * non-static. initCache() is called on construction. SegmentCache is passed
 * into constructor (therefore move ServiceDiscovery into
 * client). AggregationManager (or maybe MondrianServer) is another constructor
 * parameter.</p>
 *
 * <p>6. Move functionality Aggregation to Segment. Long-term, Aggregation
 * should not be used as a 'gatekeeper' to Segment. Remove Aggregation fields
 * columns and axes.</p>
 *
 * <p>10. Rename Aggregation.Axis to SegmentAxis.</p>
 *
 * <p>11. Remove Segment.setData and instead split out subclass
 * SegmentWithData. Now segment is immutable. You don't have to wait for its
 * state to change. You wait for a Future&lt;SegmentWithData&gt; to become
 * ready.</p>
 *
 * <p>12. Remove methods: RolapCube.checkAggregateModifications,
 * RolapStar.checkAggregateModifications,
 * RolapSchema.checkAggregateModifications,
 * RolapStar.pushAggregateModificationsToGlobalCache,
 * RolapSchema.pushAggregateModificationsToGlobalCache,
 * RolapCube.pushAggregateModificationsToGlobalCache.</p>
 *
 * <p>13. Add new implementations of Future: CompletedFuture and SlotFuture.</p>
 *
 * <p>14. Remove methods:<p>
 * <ul>
 *
 * <li>Remove {@link SegmentLoader}.loadSegmentsFromCache - creates a
 *   {@link SegmentHeader} that has PRECISELY same specification as the
 *   requested segment, very unlikely to have a hit</li>
 *
 * <li>Remove {@link SegmentLoader}.loadSegmentFromCacheRollup</li>
 *
 * <li>Break up {@link SegmentLoader}.cacheSegmentData, and
 *   place code that is called after a segment has arrived</li>
 *
 * </ul>
 *
 *
 * <h2>Done but not checked in</h2>
 *
 *
 *
 *
 * <h2>Ideas and tasks</h2>
 *
 * <p>1. Add method {@code SegmentCache.addListener(Listener)}, with
 *
 * <pre>
 * interface Listener {
 *   void segmentAdded(SegmentHeader);
 * }</pre></p>
 *
 * <p>5. Move SegmentHeader, SegmentBody, ConstrainedColumn into
 * mondrian.spi. Leave behind dependencies on mondrian.rolap.agg. In particular,
 * put code that converts Segment + SegmentWithData to and from SegmentHeader
 * + SegmentBody (e.g. {@link SegmentHeader#forSegment}) into a utility class.
 * (Do this as CLEANUP, after functionality is complete?)</p>
 *
 * <p>7. RolapStar.localAggregations and .sharedAggregations. Obsolete
 * sharedAggregations.</p>
 *
 * <p>CRUD analysis:
 * <ul>
 *
 * <li>delete: RolapStar.clearCachedAggregations;
 *     calls sharedAggregations.clear</li>
 *
 * <li>read: RolapStar.lookupAggregation(AggregationKey)
 *     calls sharedAggregations.get</li>
 *
 * <li>read into local: RolapStar.checkAggregateModifications;
 *     calls localAggregations.put on each value in sharedAggregations</li>
 *
 * <li>delete: RolapStar.flush;
 *     calls aggregation.flush for each value in sharedAggregations</li>
 *
 * <li>create: RolapStar.pushAggregateModificationsToGlobalCache;
 *     called at end of constructing RolapResult</li>
 *
 * <li>create: SegmentLoader.load;
 *     calls SegmentLoader.loadSegmentsFromCache
 *     then generates SQL if not found</li>
 *
 * </ul>
 *
 * <p>8. Longer term. Maybe move {@link RolapStar.Bar#segmentRefs} to
 * {@link mondrian.server.Execution}. Would it still be thread-local?</p>
 *
 * <p>9. Obsolete {@link RolapStar#cacheAggregations}. Similar effect will be
 * achieved by removing the 'jvm cache' from the chain of caches.</p>
 *
 * <p>10. Call
 * {@link mondrian.spi.DataSourceChangeListener#isAggregationChanged}.
 * Previously called from
 * {@link RolapStar}.checkAggregateModifications, now never called.</p>
 *
 * <p>12. Remove SegmentHeader.forCacheRegion (1 use), replace with
 * List&lt;SegmentHeader&gt;
 * SegmentCacheIndex.findHeadersOverlapping(RolapCacheRegion).
 * Then what? Various options:</p>
 *
 * <ol>
 *
 * <li>Option #1. Pull them in, trim them, write them out? But: causes
 *     a lot of I/O, and we may never use these
 *     segments. Easiest.</li>
 *
 * <li>Option #2. Mark the segments in the index as needing to be trimmed; trim
 *     them when read, and write out again. But: doesn't propagate to other
 *     nodes.</li>
 *
 * <li>Option #3. (Best?) Write a mapping SegmentHeader->Restrictions into the
 *     cache.  Less I/O than #1. Method
 *     "SegmentCache.addRestriction(SegmentHeader, CacheRegion)"</li>
 *
 * </ol>
 *
 * <p>13. Call {@link Aggregation#flush}. Not currently called. Was called
 * from {@link RolapStar#flush}.</p>
 *
 * <p>14. Move {@link AggregationManager#getCellFromCache} somewhere else.
 *   It's concerned with local segments, not the global/external cache.</p>
 *
 * <p>15. Method to convert SegmentHeader + SegmentBody to Segment +
 * SegmentWithData is imperfect. Cannot parse predicates, compound predicates.
 * Need mapping in star to do it properly and efficiently?
 * See {@link SegmentHeader#toSegment}.</p>
 *
 * <p>17. Revisit the strategy for finding segments that can be copied from
 * global and external cache into local cache. The strategy of sending N
 * {@link CellRequest}s at a time, then executing SQL to fill in the gaps, is
 * flawed. We need to maximize N in order to reduce segment fragmentation, but
 * if too high, we blow memory. BasicQueryTest.testAnalysis is an example of
 * this. Instead, we should send cell-requests in batches (is ~1000 the right
 * size?), identify those that can be answered from global or external cache,
 * return those segments, but not execute SQL until the end of the phase.
 * If so, {@link CellRequestQuantumExceededException} be obsoleted.</p>
 *
 *
 * <h2>Segment lifecycle</h2>
 *
 * <ul>
 * <li>sequence of missed cell requests &rarr; batch</li>
 * <li>send batch to CellCacheActor &rarr; future&lt;void&gt;</li>
 * </ul>
 *
 *
 * <h2>Questions for Luc</h2>
 *
 * <p>1. Is a SegmentCache supposed to be thread-safe?</p>
 * <p>no.</p>
 *
 * <p>2. SegmentCache.put - should it return {@code Future<Boolean>} or
 * {@code Future<Void>}?</p>
 * <p>whichever. doesn't really matter.</p>
 *
 * <p>3. SegmentCache.flush - too much burden on the cache provider?  Instead,
 * SegmentCacheIndex should identify segment headers that overlap with the
 * region.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public class SegmentCacheManager {
    private final Handler handler = new Handler();

    public final SegmentCacheIndex segmentIndex =
        new SegmentCacheIndexImpl(ACTOR.thread);

    private static final Actor ACTOR = new Actor();

    static {
        // Create and start thread for actor.
        //
        // Actor is shared between all servers. This reduces concurrency.
        // This might become a concern for those executing several active
        // servers in the same JVM.
        // We tried creating one actor (and therefore thread) per server, but
        // some applications (and in particular some tests) create lots of
        // servers.
        //
        // The actor is shut down with the JVM.
        final Thread thread = new Thread(ACTOR, "Mondrian segment cache");
        thread.setDaemon(true);
        thread.start();
    }

    public <T> T execute(Command<T> command) {
        return ACTOR.execute(handler, command);
    }

    /**
     * Tells the cache that a segment has completed loading from SQL, and
     * provides the data set.
     *
     * <p>Called by a SQL worker.</p>
     */
    public void loadSucceeded(
        Segment segment,
        SegmentDataset dataset)
    {
        ACTOR.event(
            handler,
            new SegmentLoadSucceededEvent(null /*bar*/, segment, dataset));
    }

    /**
     * Tells the cache that an attempt to load a segment has failed.
     *
     * <p>Called by a SQL worker.</p>
     */
    public void loadFailed(
        Segment segment,
        Throwable throwable)
    {
        ACTOR.event(
            handler,
            new SegmentLoadFailedEvent(segment, throwable));
    }

    /**
     * Adds segment to segment index and cache.
     *
     * <p>Called when a SQL statement has finished loading a segment.</p>
     *
     * @param aggMgr Aggregate manager
     * @param header segment header
     * @param body segment body
     */
    public void add(
        AggregationManager aggMgr,
        SegmentHeader header,
        SegmentBody body)
    {
        ACTOR.event(
            handler,
            new SegmentAddEvent(aggMgr, header, body));
    }

    /**
     * Tells the cache that a segment is newly available in an external cache.
     */
    public void externalSegmentCreated(
        AggregationManager aggMgr,
        SegmentHeader header)
    {
        ACTOR.event(
            handler,
            new ExternalSegmentCreatedEvent(aggMgr, header));
    }

    /**
     * Tells the cache that a segment is no longer available in an external
     * cache.
     */
    public void externalSegmentDeleted(
        AggregationManager aggMgr,
        SegmentHeader header)
    {
        ACTOR.event(
            handler,
            new ExternalSegmentDeletedEvent(aggMgr, header));
    }

    /**
     * Visitor for messages (commands and events).
     */
    public interface Visitor {
        void visit(SegmentLoadSucceededEvent event);
        void visit(SegmentLoadFailedEvent event);
        void visit(SegmentAddEvent event);
        void visit(ExternalSegmentCreatedEvent event);
        void visit(ExternalSegmentDeletedEvent event);
    }

    private static class Handler implements Visitor {
        public void visit(SegmentLoadSucceededEvent event) {
            // 1. put dataset inside segment (create new segment?) and place it
            // in index;
            // 2. inform external cache
            // 3. inform the thread that requested this segment
            // 4. inform any other threads who requested it while it was in the
            //    process of loading
            /*
            event.bar.segmentRefs.add(
                new SoftReference<Segment>(event.segment)
            );
            List<FutureTask<Segment>> tasks =
                segmentCompletionTasks.remove(event.segment);
            if (tasks != null) {
                for (FutureTask<Segment> task : tasks) {
                    task.
                }
            }
            */
            throw new UnsupportedOperationException(); // TODO:
        }

        public void visit(SegmentLoadFailedEvent event) {
            throw new UnsupportedOperationException(); // TODO:
        }

        public void visit(SegmentAddEvent event) {
            event.aggMgr.cacheMgr.segmentIndex.add(event.header);
            if (event.body == null) {
                return;
            }
            for (SegmentCacheWorker segmentCacheWorker
                : event.aggMgr.segmentCacheWorkers)
            {
                segmentCacheWorker.put(event.header, event.body);
            }
        }

        public void visit(ExternalSegmentCreatedEvent event) {
            event.aggMgr.cacheMgr.segmentIndex.add(event.header);
        }

        public void visit(ExternalSegmentDeletedEvent event) {
            event.aggMgr.cacheMgr.segmentIndex.remove(event.header);
        }
    }

    private static class SegmentLoadedTask extends FutureTask<Segment> {
        public SegmentLoadedTask(Callable<Segment> segmentCallable) {
            super(segmentCallable);
        }

        @Override
        protected void done() {
            super.done();
        }
    }

    interface Message {
    }

    public static interface Command<T> extends Message, Callable<T> {
    }

    private static class ShutdownCommand implements Command<String> {
        public String call() throws Exception {
            return "Shutdown succeeded";
        }
    }

    private static abstract class Event implements Message {
        /**
         * Dispatches a call to the appropriate {@code visit} method on
         * {@link mondrian.server.monitor.Visitor}.
         *
         * @param visitor Visitor
         */
        public abstract void acceptWithoutResponse(Visitor visitor);
    }

    /**
     * Point for various clients in a request-response pattern to receive the
     * response to their requests.
     *
     * <p>The key type should test for object identity using the == operator,
     * like {@link java.util.WeakHashMap}. This allows responses to be automatically
     * removed if the request (key) is garbage collected.</p>
     *
     * <p><b>Thread safety</b>. {@link #queue} is a thread-safe data structure;
     * a thread can safely call {@link #put} while another thread calls
     * {@link #take}. The {@link #taken} map is not thread safe, so you must
     * lock the ResponseQueue before reading or writing it.</p>
     *
     * <p>If requests are processed out of order, this queue is not ideal: until
     * request #1 has received its response, requests #2, #3 etc. will not
     * receive their response. However, this is not a problem for the monitor,
     * which uses an actor model, processing requests in strict order.</p>
     *
     * <p>REVIEW: This class is copy-pasted from
     * {@link mondrian.server.monitor.Monitor}. Consider
     * abstracting common code.</p>
     *
     * @param <K> request (key) type
     * @param <V> response (value) type
     */
    private static class ResponseQueue<K, V> {
        private final BlockingQueue<Pair<K, V>> queue;

        /**
         * Entries that have been removed from the queue. If the request
         * is garbage-collected, the map entry is removed.
         */
        private final Map<K, V> taken =
            new WeakHashMap<K, V>();

        /**
         * Creates a ResponseQueue with given capacity.
         *
         * @param capacity Capacity
         */
        public ResponseQueue(int capacity) {
            queue = new ArrayBlockingQueue<Pair<K, V>>(capacity);
        }

        /**
         * Places a (request, response) pair onto the queue.
         *
         * @param k Request
         * @param v Response
         * @throws InterruptedException if interrupted while waiting
         */
        public void put(K k, V v) throws InterruptedException {
            queue.put(Pair.of(k, v));
        }

        /**
         * Retrieves the response from the queue matching the given key,
         * blocking until it is received.
         *
         * @param k Response
         * @return Response
         * @throws InterruptedException if interrupted while waiting
         */
        public synchronized V take(K k) throws InterruptedException {
            final V v = taken.remove(k);
            if (v != null) {
                return v;
            }
            // Take the laundry out of the machine. If it's ours, leave with it.
            // If it's someone else's, fold it neatly and put it on the pile.
            for (;;) {
                final Pair<K, V> pair = queue.take();
                if (pair.left.equals(k)) {
                    return pair.right;
                } else {
                    taken.put(pair.left, pair.right);
                }
            }
        }
    }

    /**
     * Copy-pasted from {@link mondrian.server.monitor.Monitor}. Consider
     * abstracting common code.
     */
    private static class Actor implements Runnable {
        /**
         * Current thread. Not null if and only if actor is running. Can be used
         * to check that data structures are called from the dedicated thread.
         */
        private Thread thread;

        private final BlockingQueue<Pair<Handler, Message>> eventQueue =
            new ArrayBlockingQueue<Pair<Handler, Message>>(1000);

        private final ResponseQueue<Command, Pair<Object, Throwable>>
            responseQueue =
            new ResponseQueue<Command, Pair<Object, Throwable>>(1000);

        public void run() {
            thread = Thread.currentThread();
            try {
                for (;;) {
                    final Pair<Handler, Message> entry = eventQueue.take();
                    final Handler handler = entry.left;
                    final Message message = entry.right;
                    try {
                        // A message is either a command or an event.
                        // A command returns a value that must be read by
                        // the caller.
                        if (message instanceof Command) {
                            Command command = (Command) message;
                            try {
                                Object result = command.call();
                                responseQueue.put(
                                    command,
                                    Pair.of(result, (Throwable) null));
                            } catch (Throwable e) {
                                responseQueue.put(
                                    command,
                                    Pair.of(null, e));
                            }
                        } else {
                            Event event = (Event) message;
                            event.acceptWithoutResponse(handler);

                            // Broadcast the event to anyone who is interested.
                            RolapUtil.MONITOR_LOGGER.debug(message);
                        }
                    } catch (Throwable e) {
                        // REVIEW: Somewhere better to send it?
                        e.printStackTrace();
                    }
                    if (message instanceof ShutdownCommand) {
                        return;
                    }
                }
            } catch (InterruptedException e) {
                // REVIEW: Somewhere better to send it?
                e.printStackTrace();
            } finally {
                thread = null;
            }
        }

        public void shutdown() {
            // No point sending a command if (for some reason) there's no thread
            // listening to the command queue.
            if (thread != null) {
                execute(null, new ShutdownCommand());
            }
        }

        <T> T execute(Handler handler, Command<T> command) {
            try {
                eventQueue.put(Pair.<Handler, Message>of(handler, command));
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + command);
            }
            try {
                final Pair<Object, Throwable> pair =
                    responseQueue.take(command);
                if (pair.right != null) {
                    if (pair.right instanceof RuntimeException) {
                        throw (RuntimeException) pair.right;
                    } else if (pair.right instanceof Error) {
                        throw (Error) pair.right;
                    } else {
                        throw new RuntimeException(pair.right);
                    }
                } else {
                    return (T) pair.left;
                }
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + command);
            }
        }

        public void event(Handler handler, Event event) {
            try {
                eventQueue.put(Pair.<Handler, Message>of(handler, event));
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + event);
            }
        }
    }

    private static class SegmentLoadSucceededEvent extends Event {
        private final FutureTask<Segment> task;
        private final Segment segment;
        private final SegmentDataset dataset;

        public SegmentLoadSucceededEvent(
            FutureTask<Segment> task,
            Segment segment,
            SegmentDataset dataset)
        {
            this.task = task;
            this.segment = segment;
            this.dataset = dataset;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class SegmentLoadFailedEvent extends Event {
        private final Segment segment;
        private final Throwable throwable;

        public SegmentLoadFailedEvent(
            Segment segment,
            Throwable throwable)
        {
            this.segment = segment;
            this.throwable = throwable;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class SegmentAddEvent extends Event {
        private final AggregationManager aggMgr;
        private final SegmentHeader header;
        private final SegmentBody body;

        public SegmentAddEvent(
            AggregationManager aggMgr,
            SegmentHeader header,
            SegmentBody body)
        {
            assert header != null;
            assert aggMgr != null;
            this.aggMgr = aggMgr;
            this.header = header;
            this.body = body; // may be null
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class ExternalSegmentCreatedEvent extends Event {
        private final AggregationManager aggMgr;
        private final SegmentHeader header;

        public ExternalSegmentCreatedEvent(
            AggregationManager aggMgr,
            SegmentHeader header)
        {
            assert header != null;
            assert aggMgr != null;
            this.aggMgr = aggMgr;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class ExternalSegmentDeletedEvent extends Event {
        private final AggregationManager aggMgr;
        private final SegmentHeader header;

        public ExternalSegmentDeletedEvent(
            AggregationManager aggMgr,
            SegmentHeader header)
        {
            assert header != null;
            assert aggMgr != null;
            this.aggMgr = aggMgr;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }
}

// End SegmentCacheManager.java
