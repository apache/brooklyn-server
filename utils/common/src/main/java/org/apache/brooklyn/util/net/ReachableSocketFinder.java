/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.net;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * For finding an open/reachable ip:port for a node.
 */
public class ReachableSocketFinder {

    private static final Logger LOG = LoggerFactory.getLogger(ReachableSocketFinder.class);

    private final Predicate<? super HostAndPort> socketTester;
    private final ListeningExecutorService userExecutor;

    public ReachableSocketFinder(ListeningExecutorService userExecutor) {
        this(Networking.isReachablePredicate(), userExecutor);
    }

    public ReachableSocketFinder(Predicate<? super HostAndPort> socketTester, ListeningExecutorService userExecutor) {
        this.socketTester = checkNotNull(socketTester, "socketTester");
        this.userExecutor = checkNotNull(userExecutor, "userExecutor");
    }

    /**
     * Returns the first element of sockets that is reachable.
     * @param sockets The host-and-ports to test
     * @param timeout Max time to try to connect to the ip:port
     * 
     * @return The reachable ip:port
     * @throws NoSuchElementException If no ports are accessible within the given time
     * @throws NullPointerException  If sockets or timeout is null
     * @throws IllegalStateException  If the sockets to test is empty
     */
    public HostAndPort findOpenSocketOnNode(final Iterable<? extends HostAndPort> sockets, Duration timeout) {
        checkNotNull(sockets, "sockets");
        checkState(!Iterables.isEmpty(sockets), "No hostAndPort sockets supplied");
        checkNotNull(timeout, "timeout");
        LOG.debug("blocking on any reachable socket in {} for {}", sockets, timeout);
        Iterator<HostAndPort> iter = findOpenSocketsOnNode(sockets, timeout).iterator();
        if (iter.hasNext()) {
            return iter.next();
        } else {
            LOG.warn("No sockets in {} reachable after {}", sockets, timeout);
            throw new NoSuchElementException("could not connect to any socket in " + sockets);
        }
    }

    /**
     * Returns an iterable of the elements in sockets that are reachable. The order of elements
     * in the iterable corresponds to the order of the elements in the input, not the order in which their
     * reachability was determined. Iterators are unmodifiable and are evaluated lazily.
     *
     * @param sockets The host-and-ports to test
     * @param timeout Max time to try to connect to each ip:port
     * @return An iterable containing all sockets that are reachable according to {@link #socketTester}.
     * @throws NullPointerException  If sockets or timeout is null
     * @throws IllegalStateException  If the sockets to test is empty
     */
    public Iterable<HostAndPort> findOpenSocketsOnNode(final Iterable<? extends HostAndPort> sockets, Duration timeout) {
        checkNotNull(sockets, "sockets");
        checkState(!Iterables.isEmpty(sockets), "No hostAndPort sockets supplied");
        checkNotNull(timeout, "timeout");
        return Optional.presentInstances(tryReachable(sockets, timeout));
    }

    /**
     * @return A lazily computed Iterable containing present values for the elements of sockets that are
     * reachable according to {@link #socketTester} and absent values for those not. Checks are concurrent
     * and the elements in the Iterable are ordered according to their position in sockets.
     */
    private Iterable<Optional<HostAndPort>> tryReachable(Iterable<? extends HostAndPort> sockets, final Duration timeout) {
        final List<ListenableFuture<Optional<HostAndPort>>> futures = Lists.newArrayList();
        final AtomicReference<Stopwatch> sinceFirstCompleted = new AtomicReference<>();

        for (final HostAndPort socket : sockets) {
            futures.add(userExecutor.submit(new Callable<Optional<HostAndPort>>() {
                @Override
                public Optional<HostAndPort> call() {
                    // Whether the socket was reachable (vs. the result of call, which is whether the repeater is done).
                    final AtomicBoolean theResultWeCareAbout = new AtomicBoolean();
                    Repeater.create("socket-reachable")
                            .limitTimeTo(timeout)
                            .backoffTo(Duration.FIVE_SECONDS)
                            .until(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws TimeoutException {
                                    boolean reachable = socketTester.apply(socket);
                                    if (reachable) {
                                        theResultWeCareAbout.set(true);
                                        return true;
                                    } else {
                                        // Run another check if nobody else has completed yet or another task has
                                        // completed but this one is still in its grace period.
                                        Stopwatch timerSinceFirst = sinceFirstCompleted.get();
                                        return timerSinceFirst != null && Duration.FIVE_SECONDS.subtract(Duration.of(timerSinceFirst)).isNegative();
                                    }
                                }
                            })
                            .run();
                    if (theResultWeCareAbout.get()) {
                        sinceFirstCompleted.compareAndSet(null, Stopwatch.createStarted());
                    }
                    return theResultWeCareAbout.get() ? Optional.of(socket) : Optional.<HostAndPort>absent();
                }
            }));
        }

        return new Iterable<Optional<HostAndPort>>() {
            @Override
            public Iterator<Optional<HostAndPort>> iterator() {
                return new AbstractIterator<Optional<HostAndPort>>() {
                    int count = 0;

                    @Override
                    protected Optional<HostAndPort> computeNext() {
                        if (count < futures.size()) {
                            final Future<Optional<HostAndPort>> future = futures.get(count++);
                            try {
                                return future.get(timeout.toUnit(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                Exceptions.propagateIfInterrupt(e);
                                LOG.trace("Suppressed exception checking reachability of socket", e);
                            }
                            future.cancel(true);
                            return Optional.absent();
                        } else {
                            return endOfData();
                        }
                    }
                };
            }
        };
    }
}
