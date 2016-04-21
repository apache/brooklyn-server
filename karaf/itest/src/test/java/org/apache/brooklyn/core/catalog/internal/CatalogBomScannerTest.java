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
package org.apache.brooklyn.core.catalog.internal;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.IntegrationTest;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.apache.karaf.features.BootFinished;
import org.apache.karaf.features.FeaturesService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.service.cm.ConfigurationAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.Dictionary;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class CatalogBomScannerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBomScannerTest.class);
    public static final String KARAF_INIT_BLUEPRINT_BOMSCANNER_PID = "org.apache.brooklyn.core.catalog.bomscanner";

    @Inject
    protected FeaturesService featuresService;

    @Inject
    protected ConfigurationAdmin configAdmin;

    @Inject
    protected ManagementContext managementContext;

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
//             , KarafDistributionOption.debugConfiguration()
        );
    }

    @Test
    @Category(IntegrationTest.class)
    public void shouldFindWebAppCatalogExampleOnlyAfterItsFeatureIsInstalled() throws Exception {

        final CatalogItem<?, ?> catalogItem = managementContext.getCatalog()
            .getCatalogItem("load-balancer", BrooklynVersion.get());  // from brooklyn-software-webapp
        assertNull(catalogItem);

        featuresService.installFeature("brooklyn-software-webapp", BrooklynVersion.get());

        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.TEN_SECONDS), new Runnable() {
            @Override
            public void run() {
                final CatalogItem<?, ?> lb = managementContext.getCatalog()
                    .getCatalogItem("load-balancer", BrooklynVersion.get());
                assertNotNull(lb);
            }
        });
    }

    @Test
    @Category(IntegrationTest.class)
    public void shouldNotFindNoSqlCatalogExampleIfItIsBlacklisted() throws Exception {

        // verify no NoSQL entities are loaded yet
        final String riakTemplate = "bash-web-and-riak-template"; // from brooklyn-software-nosql
        CatalogItem<?, ?> catalogItem = getCatalogItem(riakTemplate);
        assertNull(catalogItem);

        final String redisStore = "org.apache.brooklyn.entity.nosql.redis.RedisStore"; // ditto
        catalogItem = getCatalogItem(redisStore);
        assertNull(catalogItem);

        // blacklist the org.apache.brooklyn.software-nosql bundle
        final org.osgi.service.cm.Configuration bomScannerConfig =
            configAdmin.getConfiguration(KARAF_INIT_BLUEPRINT_BOMSCANNER_PID);
        final Dictionary<String, Object> bomProps = bomScannerConfig.getProperties();
        assertEquals(".*", bomProps.get("whiteList"));
        assertEquals("", bomProps.get("blackList"));

        bomProps.put("blackList", ".*nosql.*");
        bomScannerConfig.update(bomProps);

        // install the NoSQL feature
        featuresService.installFeature("brooklyn-software-nosql", BrooklynVersion.get());

        // verify that the non-template entity org.apache.brooklyn.entity.nosql.redis.RedisStore gets added to catalog
        verifyCatalogItemEventually(redisStore, true);

        // verify that the template application hasn't made it into the catalog (because it's blacklisted)
        catalogItem = getCatalogItem(riakTemplate);
        assertNull(catalogItem);

        // For completeness let's uninstall the bundle, un-blacklist nosql, and install again
        featuresService.uninstallFeature("brooklyn-software-nosql", BrooklynVersion.get());

        // verify it's gone away
        verifyCatalogItemEventually(redisStore, false);

        // un-blacklist nosql
        bomProps.put("blackList", "");
        bomScannerConfig.update(bomProps);

        // install it again
        featuresService.installFeature("brooklyn-software-nosql", BrooklynVersion.get());

        // now the application should make it into the catalog
        verifyCatalogItemEventually(redisStore, true);

    }

    private void verifyCatalogItemEventually(final String redisStore, final boolean isItThere) {
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.TEN_SECONDS), new Runnable() {
            @Override
            public void run() {
                final CatalogItem<?, ?> redis = getCatalogItem(redisStore);
                assertEquals(null != redis, isItThere);
            }
        });
    }

    private CatalogItem<?, ?> getCatalogItem(String itemName) {
        return managementContext.getCatalog().getCatalogItem(itemName, BrooklynVersion.get());
    }

}
