/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.performance;

import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * For running simplistic performance tests, to measure the number of operations per second.
 * 
 * With a short run, this is "good enough" for eye-balling performance, to spot if it goes 
 * horrendously wrong. 
 * 
 * However, good performance measurement involves much more warm up (e.g. to ensure java HotSpot 
 * optimisation have been applied), and running the test for a reasonable length of time.
 * 
 * Longevity tests are also important for to check if object creation is going to kill
 * performance in the long-term, etc.
 */
@Beta
public class PerformanceMeasurer {

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceMeasurer.class);

    /**
     * Runs a performance test. Repeatedly executes the given job. It measuring either the time it takes for
     * many iterations, or the number of iterations it can execute in a fixed time.
     */
    public static PerformanceTestResult run(PerformanceTestDescriptor options) {
        options.seal();
        long nextLogTime = (options.logInterval == null) ? Long.MAX_VALUE : options.logInterval.toMilliseconds();
        
        // Try to force garbage collection before the test, so it interferes less with the measurement.
        System.gc(); System.gc();
        
        // Run some warm-up cycles.
        Stopwatch warmupWatch = Stopwatch.createStarted();
        int warmupCounter = 0;
        
        while ((options.warmup != null) ? options.warmup.isLongerThan(warmupWatch) : warmupCounter < options.warmupIterations) {
            if (warmupWatch.elapsed(TimeUnit.MILLISECONDS) >= nextLogTime) {
                LOG.info("Warm-up "+options.summary+" iteration="+warmupCounter+" at "+Time.makeTimeStringRounded(warmupWatch));
                nextLogTime += options.logInterval.toMilliseconds();
            }
            if (options.preJob != null) options.preJob.run();
            options.job.run();
            if (options.postJob != null) options.postJob.run();
            warmupCounter++;
        }
        warmupWatch.stop();
        
        if (options.postWarmup != null) options.postWarmup.run();
        
        // Run the actual test (for the given duration / iterations); then wait for completionLatch (if supplied).
        nextLogTime = (options.logInterval == null) ? Long.MAX_VALUE : options.logInterval.toMilliseconds();
        int counter = 0;
        Histogram histogram = new Histogram();
        List<Double> cpuSampleFractions = Lists.newLinkedList();
        Future<?> sampleCpuFuture = null;
        if (options.sampleCpuInterval != null) {
            sampleCpuFuture = PerformanceTestUtils.sampleProcessCpuTime(options.sampleCpuInterval, options.summary, cpuSampleFractions);
        }
        
        int numConcurrentJobs = options.numConcurrentJobs;
        ListeningExecutorService executorService = null;
        if (numConcurrentJobs > 1) {
            executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numConcurrentJobs));
        }
        
        PerformanceTestResult result = null;
        try {
            long preCpuTime = PerformanceTestUtils.getProcessCpuTime();
            Stopwatch watch = Stopwatch.createStarted();
            
            try {
                while ((options.duration != null) ? options.duration.isLongerThan(watch) : counter < options.iterations) {
                    if (watch.elapsed(TimeUnit.MILLISECONDS) >= nextLogTime) {
                        LOG.info(options.summary+" iteration="+counter+" at "+Time.makeTimeStringRounded(watch));
                        nextLogTime += options.logInterval.toMilliseconds();
                    }
                    
                    if (options.preJob != null) {
                        watch.stop();
                        options.preJob.run();
                        watch.start();
                    }
    
                    long before = watch.elapsed(TimeUnit.NANOSECONDS);
                    if (numConcurrentJobs > 1) {
                        runConcurrentAndBlock(executorService, options.job, numConcurrentJobs);
                    } else {
                        options.job.run();
                    }
                    Duration iterationDuration = Duration.of(watch.elapsed(TimeUnit.NANOSECONDS) - before, TimeUnit.NANOSECONDS);
                    
                    if (options.histogram) {
                        histogram.add(iterationDuration);
                    }
                    counter++;
                    
                    if (options.postJob != null) {
                        watch.stop();
                        options.postJob.run();
                        watch.start();
                    }
                    
                    if (options.abortIfIterationLongerThan != null && options.abortIfIterationLongerThan.isShorterThan(iterationDuration)) {
                        fail("Iteration "+(counter-1)+" took "+iterationDuration+", which is longer than max permitted "+options.abortIfIterationLongerThan+" for "+options);
                    }
                }
                
                if (options.completionLatch != null) {
                    try {
                        boolean success = options.completionLatch.await(options.completionLatchTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                        if (!success) {
                            fail("Timeout waiting for completionLatch: test="+options+"; counter="+counter);
                        }
                    } catch (InterruptedException e) {
                        throw Exceptions.propagate(e);
                    } 
                }
            } finally {
                watch.stop();
                long postCpuTime = PerformanceTestUtils.getProcessCpuTime();
    
                // Generate the results
                result = new PerformanceTestResult();
                result.warmup = Duration.of(warmupWatch);
                result.warmupIterations = warmupCounter;
                result.duration = Duration.of(watch);
                result.iterations = counter;
                result.ratePerSecond = (((double)counter) / watch.elapsed(TimeUnit.MILLISECONDS)) * 1000;
                result.cpuTotalFraction = (watch.elapsed(TimeUnit.NANOSECONDS) > 0 && preCpuTime >= 0) 
                        ? ((double)postCpuTime-preCpuTime) / watch.elapsed(TimeUnit.NANOSECONDS) 
                        : -1;
                if (options.histogram) {
                    result.histogram = histogram;
                }
                if (options.sampleCpuInterval != null) {
                    result.cpuSampleFractions = cpuSampleFractions;
                }
                result.minAcceptablePerSecond = options.minAcceptablePerSecond;
            }
            
        } catch (Throwable t) {
            LOG.warn("Test failed; partial results before failure: test="+options+"; result="+result);
            throw Exceptions.propagate(t);
            
        } finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
            if (sampleCpuFuture != null) {
                sampleCpuFuture.cancel(true);
            }
        }
        
        // Persist the results
        if (options.persister != null) {
            options.persister.persist(new Date(), options, result);
        }

        // Fail if we didn't meet the minimum performance requirements
        if (options.minAcceptablePerSecond != null && options.minAcceptablePerSecond > result.ratePerSecond) {
            fail("Performance too low: test="+options+"; result="+result);
        }
        
        return result;
    }
    
    protected static void runConcurrentAndBlock(ListeningExecutorService executor, Runnable job, int numConcurrentJobs) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<ListenableFuture<?>>(numConcurrentJobs);
        for (int i = 0; i < numConcurrentJobs; i++) {
            ListenableFuture<?> future = executor.submit(job);
            futures.add(future);
            Futures.allAsList(futures).get();
        }
    }
}
