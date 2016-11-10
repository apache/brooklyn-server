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
package org.apache.brooklyn.util.javalang;

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Range;

public class MemoryUsageTrackerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageTrackerTest.class);

    @Test(groups="Integration")
    public void testBigUsage() {
        final int ALLOCATION_CHUNK_SIZE = 10*1000*1000; // 10MB
        
        // Don't just use runtime.maxMemory()*2; javadoc says:
        //     If there is no inherent limit then the value java.lang.Long.MAX_VALUE will be returned.
        // Therefore cap at 10GB.
        final long MAX_MEMORY_CAP = 10*1024*1024*1024L; // 10GB
        final long maxMemory = Math.min(Runtime.getRuntime().maxMemory(), MAX_MEMORY_CAP);
        
        List<Maybe<byte[]>> references = MutableList.of();
        long created = 0;
        while (created < 2*maxMemory) {
            byte d[] = new byte[ALLOCATION_CHUNK_SIZE];
            references.add(Maybe.soft(d));
            MemoryUsageTracker.SOFT_REFERENCES.track(d, d.length);
            created += d.length;
            
            long totalMemory = Runtime.getRuntime().totalMemory();
            long freeMemory = Runtime.getRuntime().freeMemory();
            
            LOG.info("created "+Strings.makeSizeString(created) +
                " ... in use: "+Strings.makeSizeString(totalMemory - freeMemory)+" / " +
                Strings.makeSizeString(totalMemory) +
                " ... reclaimable: "+Strings.makeSizeString(MemoryUsageTracker.SOFT_REFERENCES.getBytesUsed()) +
                " ... live refs: "+Strings.makeSizeString(sizeOfActiveReferences(references)) +
                " ... maxMem="+maxMemory+"; totalMem="+totalMemory+"; usedMem="+(totalMemory-freeMemory));
        }
        
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                long totalMemory = Runtime.getRuntime().totalMemory();
                long freeMemory = Runtime.getRuntime().freeMemory();
                long usedMemory = totalMemory - freeMemory;
                assertLessThan(MemoryUsageTracker.SOFT_REFERENCES.getBytesUsed(), maxMemory);
                assertLessThan(MemoryUsageTracker.SOFT_REFERENCES.getBytesUsed(), totalMemory);
                assertLessThan(MemoryUsageTracker.SOFT_REFERENCES.getBytesUsed(), usedMemory);
            }});
    }

    private long sizeOfActiveReferences(List<Maybe<byte[]>> references) {
        long size = 0;
        for (Maybe<byte[]> ref: references) {
            byte[] deref = ref.orNull();
            if (deref!=null) size += deref.length;
        }
        return size;
    }

    @Test(groups="Integration")
    public void testSoftUsageAndClearance() {
        MemoryUsageSummary initialMemory = new MemoryUsageSummary();
        LOG.info("Memory usage at start of test: "+initialMemory);
        
        MemoryUsageSummary beforeCollectedMemory = null;
        
        List<Maybe<?>> dump = MutableList.of();
        for (int i=0; i<1000*1000; i++) {
            beforeCollectedMemory = new MemoryUsageSummary();
            
            dump.add(Maybe.soft(new byte[1000*1000]));
            if (containsAbsent(dump)) break;
        }
        int cleared = countAbsents(dump);
        LOG.info("First soft reference cleared after "+dump.size()+" 1M blocks created; "+cleared+" of them cleared; memory just before collected is "+beforeCollectedMemory);
        
        // Expect the soft references to only have been collected when most of the JVM's memory 
        // was being used. However, it's not necessarily "almost all" (e.g. I've seen on my 
        // laptop the above log message show usedFraction=0.8749949398012845).
        // For more details of when this would be triggered, see:
        //     http://jeremymanson.blogspot.co.uk/2009/07/how-hotspot-decides-to-clear_07.html
        // And note that we set `-XX:SoftRefLRUPolicyMSPerMB=1` to avoid:
        //     https://issues.apache.org/jira/browse/BROOKLYN-375
        assertUsedMemoryFractionWithinRange(beforeCollectedMemory, Range.closed(0.7, 1.0));
        
        String clearanceResult = MemoryUsageTracker.forceClearSoftReferences(100*1000, 10*1000*1000);
        LOG.info("Forcing memory eviction: " + clearanceResult);
        
        System.gc(); System.gc();
        MemoryUsageSummary afterClearedMemory = new MemoryUsageSummary();
        double initialUsedFraction = 1.0*initialMemory.used / afterClearedMemory.total; // re-calculate; might have grown past -Xms during test.
        assertUsedMemoryFractionWithinRange(afterClearedMemory, Range.closed(0.0, initialUsedFraction + 0.1));
        LOG.info("Final memory usage (after forcing clear, and GC): "+afterClearedMemory);
    }
    
    private static class MemoryUsageSummary {
        final long total;
        final long free;
        final long used;
        final double usedFraction;
        
        MemoryUsageSummary() {
            total = Runtime.getRuntime().totalMemory();
            free = Runtime.getRuntime().freeMemory();
            used = total - free;
            usedFraction = 1.0*used / total;
        }
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("total", Strings.makeSizeString(total))
                    .add("free", Strings.makeSizeString(free))
                    .add("used", Strings.makeSizeString(used))
                    .add("usedFraction", usedFraction)
                    .toString();
        }
    }

    private void assertUsedMemoryFractionWithinRange(MemoryUsageSummary actual, Range<Double> expectedRange) {
        assertTrue(expectedRange.contains(actual.usedFraction), "actual="+actual+"; expectedRange="+expectedRange);
    }
    
    private boolean containsAbsent(Iterable<Maybe<?>> objs) {
        for (Maybe<?> obj : objs) {
            if (obj.isAbsent()) return true;
        }
        return false;
    }
    
    private int countAbsents(Iterable<Maybe<?>> objs) {
        int result = 0;
        for (Maybe<?> obj : objs) {
            if (obj.isAbsent()) result++;
        }
        return result;
    }
    
    private static void assertLessThan(long lhs, long rhs) {
        Assert.assertTrue(lhs<rhs, "Expected "+lhs+" < "+rhs);
    }
    
}
