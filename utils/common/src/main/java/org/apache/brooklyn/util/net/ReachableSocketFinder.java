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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * For finding an open/reachable ip:port for a node.
 */
public class ReachableSocketFinder {

    private static final Logger LOG = LoggerFactory.getLogger(ReachableSocketFinder.class);

    private final Predicate<? super HostAndPort> socketTester;
    private final Duration gracePeriod;

    public ReachableSocketFinder() {
        this(Networking.isReachablePredicate());
    }

    public ReachableSocketFinder(Predicate<? super HostAndPort> socketTester) {
        this(socketTester, Duration.FIVE_SECONDS);
    }

    /**
     * @param socketTester A predicate that determines the reachability of a host and port
     * @param gracePeriod The duration to allow remaining checks to continue after a socket is reached for the first time.
     */
    public ReachableSocketFinder(Predicate<? super HostAndPort> socketTester, Duration gracePeriod) {
        this.socketTester = checkNotNull(socketTester, "socketTester");
        this.gracePeriod = checkNotNull(gracePeriod, "gracePeriod");
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
        Iterable<HostAndPort> reachable = Optional.presentInstances(tryReachable(sockets, timeout, false));
        Iterator<HostAndPort> iter = reachable.iterator();
        if (iter.hasNext()) {
            return iter.next();
        } else {
            LOG.warn("No sockets in {} reachable after {}", sockets, timeout);
            throw new NoSuchElementException("Could not connect to any socket in " + sockets);
        }
    }

    /**
     * Returns an iterable of the elements in sockets that are reachable. The order of elements
     * in the iterable corresponds to the order of the elements in the input, not the order in which their
     * reachability was determined. Iterators are unmodifiable.
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
        return Optional.presentInstances(tryReachable(sockets, timeout, true));
    }

    /**
     * @return An Iterable containing present values for the elements of sockets that are reachable
     * according to {@link #socketTester} and absent values for those not. Checks are concurrent.
     * The iterable returned is ordered according sockets.
     */
    private Iterable<Optional<HostAndPort>> tryReachable(
            Iterable<? extends HostAndPort> sockets, final Duration timeout, final boolean useGracePeriod) {
        LOG.debug("Blocking on reachable sockets in {} for {}", sockets, timeout);
        final List<ListenableFuture<Optional<HostAndPort>>> futures = Lists.newArrayList();
        final AtomicReference<Stopwatch> sinceFirstCompleted = new AtomicReference<>();
        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

        for (final HostAndPort socket : sockets) {
            futures.add(executor.submit(new Callable<Optional<HostAndPort>>() {
                Future<Boolean> checker;

                private void reschedule() {
                    checker = executor.submit(new SocketChecker(socket, socketTester));
                }

                private boolean gracePeriodExpired() {
                    Stopwatch firstCompleted = sinceFirstCompleted.get();
                    return firstCompleted != null
                            && (!useGracePeriod || gracePeriod.subtract(Duration.of(firstCompleted)).isNegative());
                }

                /** Checks checker for completion and reschedules it if time allows. */
                private boolean isComplete() throws ExecutionException, InterruptedException {
                    final boolean currentCheckComplete = checker.isDone();
                    if (currentCheckComplete && checker.get()) {
                        LOG.trace("{} determined that {} is reachable", this, socket);
                        sinceFirstCompleted.compareAndSet(null, Stopwatch.createStarted());
                        return true;
                    } else if (currentCheckComplete) {
                        LOG.trace("{} unsure if {} is reachable, scheduling another check", this, socket);
                        reschedule();
                    }
                    return false;
                }

                @Override
                public Optional<HostAndPort> call() throws Exception {
                    LOG.trace("Checking reachability of {}", socket);
                    reschedule();
                    Repeater.create()
                            .limitTimeTo(timeout)
                            .backoffTo(Duration.FIVE_SECONDS)
                            .until(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    return isComplete() || gracePeriodExpired();
                                }
                            })
                            .run();
                    if (checker.isDone() && checker.get()) {
                        LOG.trace("Finished checking reachability of {}: success", socket);
                        return Optional.of(socket);
                    } else {
                        LOG.trace("Finished checking reachability of {}: failure", socket);
                        checker.cancel(true);
                        return Optional.absent();
                    }
                }
            }));
        }

        ImmutableList.Builder<Optional<HostAndPort>> results = ImmutableList.builder();
        for (ListenableFuture<Optional<HostAndPort>> f : futures) {
            try {
                // Each future uses the timeout so we don't need to include it here.
                results.add(f.get());
            } catch (InterruptedException e) {
                throw new RuntimeInterruptedException(e);
            } catch (ExecutionException e) {
                LOG.trace("Suppressed exception waiting for " + f, e);
            }
        }
        executor.shutdownNow();
        List<Optional<HostAndPort>> builtList = results.build();
        LOG.debug("Determined reachability of sockets {}: {}", sockets, builtList);
        return builtList;
    }

    private static class SocketChecker implements Callable<Boolean> {
        final HostAndPort socket;
        final Predicate<? super HostAndPort> predicate;

        private SocketChecker(HostAndPort socket, Predicate<? super HostAndPort> predicate) {
            this.socket = socket;
            this.predicate = predicate;
        }

        @Override
        public Boolean call() {
            return predicate.apply(socket);
        }
    }

}
