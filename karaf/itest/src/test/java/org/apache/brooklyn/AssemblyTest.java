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
package org.apache.brooklyn;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.inject.Inject;

import org.apache.karaf.features.BootFinished;
import org.apache.karaf.features.FeaturesService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.framework.BundleContext;

/**
 * Tests the apache-brooklyn karaf runtime assembly.
 * 
 * Keeping it a non-integration test so we have at least a basic OSGi sanity check. (takes 14 sec)
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class AssemblyTest {

    @Inject
    private BundleContext bc;

    @Inject
    @Filter(timeout = 120000)
    protected FeaturesService featuresService;

    /**
     * To make sure the tests run only when the boot features are fully
     * installed
     */
    @Inject
    @Filter(timeout = 120000)
    BootFinished bootFinished;

    @Configuration
    public static Option[] configuration() throws Exception {
        return defaultOptionsWith(
            // Uncomment this for remote debugging the tests on port 5005
            // KarafDistributionOption.debugConfiguration()
        );
    }

    @Test
    public void shouldHaveBundleContext() {
        assertNotNull(bc);
    }

    @Test
    public void checkEventFeature() throws Exception {
        assertTrue(featuresService.isInstalled(featuresService.getFeature("eventadmin")));
    }

    @Test
    public void checkBrooklynCoreFeature() throws Exception {
        featuresService.installFeature("brooklyn-core");
        assertTrue(featuresService.isInstalled(featuresService.getFeature("brooklyn-core")));
    }

}
