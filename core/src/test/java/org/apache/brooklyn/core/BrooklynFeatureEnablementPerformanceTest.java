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
package org.apache.brooklyn.core;

import static org.testng.Assert.assertFalse;

import org.apache.brooklyn.core.test.qa.performance.AbstractPerformanceTest;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.testng.annotations.Test;

public class BrooklynFeatureEnablementPerformanceTest extends AbstractPerformanceTest {

    protected int numIterations() {
        return 10000;
    }
    
    /**
     * Expect this to be blazingly fast; double-checking because it's not efficiently written:
     * <ul>
     *   <li>It doesn't cache the System.getProperty result, but I'd expect the JVM to do that!
     *   <li>It synchronizes on every access (rather than using a more efficient immutable copy/cache
     *       for example, which is then replaced atomically by a new immutable cache).
     * </ul>
     */
    @Test(groups={"Integration", "Acceptance"})
    public void testIsEnabled() {
        int numIterations = numIterations();
        double minRatePerSec = 100000 * PERFORMANCE_EXPECTATION;
        final String featureProperty = "brooklyn.experimental.feature.testIsEnabled.performance";
        
        measure(PerformanceTestDescriptor.create()
                .summary("EntityPerformanceTest.testUpdateAttributeWhenNoListeners")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .job(new Runnable() {
                    public void run() {
                        assertFalse(BrooklynFeatureEnablement.isEnabled(featureProperty));
                    }}));
    }
}
