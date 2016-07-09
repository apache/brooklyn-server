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
package org.apache.brooklyn.launcher.osgi;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.brooklyn.KarafTestUtils;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.IntegrationTest;
import org.apache.karaf.features.BootFinished;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.service.cm.ConfigurationAdmin;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Category(IntegrationTest.class)
public class OsgiLauncherTest {

    private static final String TEST_VALUE_RUNTIME = "test.value";
    private static final String TEST_KEY_RUNTIME = "test.key";
    private static final String TEST_VALUE_IN_CFG = "test.cfg";
    private static final String TEST_KEY_IN_CFG = "test.key.in.cfg";

    @Inject
    @Filter(timeout = 120000)
    protected ManagementContext mgmt;

    @Inject
    @Filter(timeout = 120000)
    protected ConfigurationAdmin configAdmin;

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
                editConfigurationFilePut("etc/org.apache.brooklyn.osgilauncher.cfg", "globalBrooklynPropertiesFile", ""),
                editConfigurationFilePut("etc/org.apache.brooklyn.osgilauncher.cfg", "localBrooklynPropertiesFile", ""),
                editConfigurationFilePut("etc/brooklyn.cfg", TEST_KEY_IN_CFG, TEST_VALUE_IN_CFG),
                features(KarafTestUtils.brooklynFeaturesRepository(), "brooklyn-osgi-launcher")
                // Uncomment this for remote debugging the tests on port 5005
                // ,KarafDistributionOption.debugConfiguration()
        );
    }

    @Test
    public void testConfig() throws IOException {
        assertFalse(mgmt.getConfig().getAllConfig().containsKey(TEST_KEY_RUNTIME));
        org.osgi.service.cm.Configuration config = configAdmin.getConfiguration("brooklyn", null);
        assertEquals(config.getProperties().get(TEST_KEY_IN_CFG), TEST_VALUE_IN_CFG);
        config.getProperties().put(TEST_KEY_RUNTIME, TEST_VALUE_RUNTIME);
        config.update();
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(TEST_VALUE_RUNTIME, mgmt.getConfig().getFirst(TEST_KEY_RUNTIME));
            }
        });
    }
}
