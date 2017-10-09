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
package org.apache.brooklyn.core.test.qa.performance;

import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.performance.PerformanceMeasurer;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.apache.brooklyn.test.performance.PerformanceTestResult;
import org.apache.brooklyn.util.internal.DoubleSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

/**
 * For running simplistic performance tests, to measure the number of operations per second and compare 
 * it against some min rate.
 * 
 * This is "good enough" for eye-balling performance, to spot if it goes horrendously wrong. 
 * 
 * However, good performance measurement involves much more warm up (e.g. to ensure java HotSpot 
 * optimisation have been applied), and running the test for a reasonable length of time.
 * We are also not running the tests for long enough to check if object creation is going to kill
 * performance in the long-term, etc.
 */
public class AbstractPerformanceTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPerformanceTest.class);
    
    public static final DoubleSystemProperty PERFORMANCE_EXPECTATION_SYSPROP = 
            new DoubleSystemProperty("brooklyn.test.performanceExpectation");
    
    /**
     * A scaling factor for the expected performance, where 1 is a conservative expectation of
     * minimum to expect every time in normal circumstances.
     * 
     * However, for running in CI, defaults to 0.1 so if GC kicks in during the test we won't fail...
     */
    public static double PERFORMANCE_EXPECTATION = PERFORMANCE_EXPECTATION_SYSPROP.isAvailable() ? 
            PERFORMANCE_EXPECTATION_SYSPROP.getValue() : 0.1d;
    
    protected static final long TIMEOUT_MS = 10*1000;
    
    protected SimulatedLocation loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        for (int i = 0; i < 5; i++) System.gc();
        loc = app.newSimulatedLocation();
    }

    @Override
    protected BrooklynProperties getBrooklynProperties() {
        if (useLiveManagementContext()) {
            return BrooklynProperties.Factory.newDefault();
        } else {
            return BrooklynProperties.Factory.newEmpty();
        }
    }
    
    /**
     * For overriding; controls whether to load ~/.brooklyn/brooklyn.properties
     */
    protected boolean useLiveManagementContext() {
        return false;
    }

    protected PerformanceTestResult measure(PerformanceTestDescriptor options) {
        PerformanceTestResult result = PerformanceMeasurer.run(options);
        LOG.info("test="+options+"; result="+result);
        return result;
    }
}
