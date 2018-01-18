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
package org.apache.brooklyn.rest.resources;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.core.GenericType;

import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.qa.performance.AbstractPerformanceTest;
import org.apache.brooklyn.enricher.stock.Aggregator;
import org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy;
import org.apache.brooklyn.rest.domain.CatalogEnricherSummary;
import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.domain.CatalogItemSummary;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.CatalogPolicySummary;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;

//by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
@Test(suiteName = "CatalogResourcePerformanceTest")
public class CatalogResourcePerformanceTest extends BrooklynRestResourcePerformanceTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CatalogResourcePerformanceTest.class);
    
    private static String TEST_VERSION = "0.1.2";

    @Override
    protected boolean useLocalScannedCatalog() {
        return true;
    }
    
    protected int numIterations() {
        return 100;
    }
    
    @Override
    protected void initClass() throws Exception {
        super.initClass();
        
        // pre-populate the catalog with YAML blueprints
        for (int i = 0; i < 10; i++) {
            String yaml = Joiner.on("\n").join(
                    "brooklyn.catalog:",
                    "  version: " + TEST_VERSION,
                    "  items:",
                    "  - id: myentity-" + i,
                    "    itemType: entity",
                    "    name: My Catalog Entity " + i,
                    "    description: My description",
                    "    icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                    "    item:",
                    "      type: " + TestEntity.class.getName(),
                    "  - id: myapp-" + i,
                    "    itemType: template",
                    "    name: My Catalog App " + i,
                    "    description: My description",
                    "    icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                    "    item:",
                    "      services:",
                    "      - type: " + TestEntity.class.getName(),
                    "  - id: mylocation-" + i,
                    "    itemType: location",
                    "    name: My Catalog Location " + i,
                    "    description: My description",
                    "    item:",
                    "      type: localhost",
                    "  - id: mypolicy-" + i,
                    "    itemType: policy",
                    "    name: My Catalog Policy " + i,
                    "    description: My description",
                    "    item:",
                    "      type: " + AutoScalerPolicy.class.getName(),
                    "  - id: myenricher-" + i,
                    "    itemType: enricher",
                    "    name: My Catalog Enricher " + i,
                    "    description: My description",
                    "    item:",
                    "      type: " + Aggregator.class.getName());
            getManagementContext().getCatalog().addItems(yaml);
        }
    }

    @Test(groups={"Integration"})
    public void testListAllEntities() {
        runListAllPerformanceTest("testListAllEntities", "/catalog/entities", new GenericType<List<CatalogEntitySummary>>() {});
    }
    
    @Test(groups={"Integration"})
    public void testListAllApplications() {
        runListAllPerformanceTest("testListAllApplications", "/catalog/applications", new GenericType<List<CatalogItemSummary>>() {});
    }
    
    @Test(groups={"Integration"})
    public void testListAllPolicies() {
        runListAllPerformanceTest("testListAllPolicicies", "/catalog/policies", new GenericType<List<CatalogPolicySummary>>() {});
    }

    @Test(groups={"Integration"})
    public void testListAllEnrichers() {
        runListAllPerformanceTest("testListAllEnrichers", "/catalog/enrichers", new GenericType<List<CatalogEnricherSummary>>() {});
    }
    
    @Test(groups={"Integration"})
    public void testListAllLocations() {
        runListAllPerformanceTest("testListAllLocations", "/catalog/locations", new GenericType<List<CatalogLocationSummary>>() {});
    }
    
    @Test(groups={"Integration"})
    public void testGetEntity() {
        runGetOnePerformanceTest("testGetEntity", "/catalog/entities/myentity-0/"+TEST_VERSION, CatalogEntitySummary.class);
    }
    
    @Test(groups={"Integration"})
    public void testGetApplication() {
        runGetOnePerformanceTest("testGetApplication", "/catalog/applications/myapp-0/"+TEST_VERSION, CatalogItemSummary.class);
    }
    
    @Test(groups={"Integration"})
    public void testGetPolicy() {
        runGetOnePerformanceTest("testGetPolicy", "/catalog/policies/mypolicy-0/"+TEST_VERSION, CatalogPolicySummary.class);
    }
    
    @Test(groups={"Integration"})
    public void testGetLocation() {
        runGetOnePerformanceTest("testGetLocation", "/catalog/locations/mylocation-0/"+TEST_VERSION, CatalogLocationSummary.class);
    }
    
    protected void runListAllPerformanceTest(String methodName, final String urlPath, final GenericType<? extends List<?>> returnType) {
        runPerformanceTest(
                "CatalogResourcePerformanceTest."+methodName, 
                new Function<WebClient, Void>() {
                    @Override
                    public Void apply(WebClient client) {
                        List<?> result = client.path(urlPath).get(returnType);
                        assertTrue(result.size() > 0, "size="+result.size());
                        return null;
                    }});
    }
    
    protected void runGetOnePerformanceTest(String methodName, final String urlPath, final Class<?> returnType) {
        runPerformanceTest(
                "CatalogResourcePerformanceTest."+methodName, 
                new Function<WebClient, Void>() {
                    @Override
                    public Void apply(WebClient client) {
                        Object result = client.path(urlPath).get(returnType);
                        assertNotNull(result);
                        return null;
                    }});
    }
    
    protected void runPerformanceTest(String summary, final Function<WebClient, Void> job) {
        int numIterations = numIterations();
        double minRatePerSec = 10 * AbstractPerformanceTest.PERFORMANCE_EXPECTATION;
        
        final AtomicReference<WebClient> client = new AtomicReference<>();
        
        measure(PerformanceTestDescriptor.create()
                .summary(summary)
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .preJob(new Runnable() {
                    @Override
                    public void run() {
                        client.set(client());
                    }})
                .job(new Runnable() {
                    @Override
                    public void run() {
                        job.apply(client.get());
                    }}));
    }
}
