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
package org.apache.brooklyn.core.mgmt.osgi;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.BrooklynGarbageCollector;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.MemoryUsageTracker;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.time.Duration;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;


/** 
 * Tests that starting with the OSGi subsystem 
 * (a) does not leak memory (which should fix test failures in jenkins), and
 * (b) is fast (or we can make it faster...)
 */ 
public class OsgiTestingLeaksAndSpeedTest implements OsgiTestResources {

    private static final Logger log = LoggerFactory.getLogger(OsgiTestingLeaksAndSpeedTest.class);
    
    protected LocalManagementContext mgmt;
    protected TestApplication app;

    void up() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true)
            .enableOsgiReusable()
//            .disableOsgi()
//            .enableOsgiNonReusable()
            .build();
        app = TestApplication.Factory.newManagedInstanceForTests(mgmt);
    }

    void down() throws BundleException, IOException, InterruptedException {
        Entities.destroyAll(mgmt);
    }
    
    @Test(groups="Integration")
    public void testUpDownManyTimes() throws Exception {
        final int NUM_ITERS = 10;
        // do a couple beforehand to eliminate one-off expenses
        up(); down(); up(); down();
        forceGc();
        long memUsed0 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        Stopwatch sF = Stopwatch.createUnstarted();
        Stopwatch sG = Stopwatch.createStarted();
        for (int i=0; i<NUM_ITERS; i++) {
            log.info(JavaClassNames.niceClassAndMethod()+" iteration "+i+": "+
                BrooklynGarbageCollector.makeBasicUsageString());
            sF.start();
            up();
            // confirmed this has no effect, even with OSGi
            // Time.sleep(Duration.millis(200));
            down();
            sF.stop();
            forceGc();
        }
        forceGc();
        sG.stop();
        long memUsed1 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info(JavaClassNames.niceClassAndMethod()+" AFTER "+NUM_ITERS+": "+
            BrooklynGarbageCollector.makeBasicUsageString());
        long memLeakBytesPerIter = (memUsed1-memUsed0)/NUM_ITERS;
        long upDownTimeMillisPerIter = sF.elapsed(TimeUnit.MILLISECONDS)/NUM_ITERS;
        log.info(JavaClassNames.niceClassAndMethod()+" PER ITER (over "+NUM_ITERS+"): "+
                ByteSizeStrings.metric().makeSizeString( memLeakBytesPerIter )+", "+
                Duration.millis(upDownTimeMillisPerIter)+" per up+down, "+
                Duration.millis(sG.elapsed(TimeUnit.MILLISECONDS)/NUM_ITERS)+" per up+down+gc"
            );

        // with OSGi reusable (new)
        // PER ITER (over 10): 1156 B, 23ms per up+down, 240ms per up+down+gc
        // PER ITER (over 100): 727 B, 18ms per up+down, 249ms per up+down+gc
        
        // with OSGi non-reusable
        // PER ITER (over 10): 3.60 MB, 276ms per up+down, 642ms per up+down+gc
        // PER ITER (over 100): 1692 kB, 222ms per up+down, 988ms per up+down+gc
        
        // without OSGi
        // PER ITER (over 10): 605 B, 24ms per up+down, 185ms per up+down+gc
        // PER ITER (over 100): 747 B, 16ms per up+down, 184ms per up+down+gc
        
        Assert.assertTrue(memLeakBytesPerIter < 100*1000, "Leaked too much memory: "+memLeakBytesPerIter);
        Assert.assertTrue(upDownTimeMillisPerIter < 200, "Took too long to startup: "+upDownTimeMillisPerIter);
        
        // can keep up to attach debugger
//        Time.sleep(Duration.ONE_DAY);
    }

    protected void forceGc() {
        MemoryUsageTracker.forceClearSoftReferences();
        System.gc(); System.gc();
    }
            
}
