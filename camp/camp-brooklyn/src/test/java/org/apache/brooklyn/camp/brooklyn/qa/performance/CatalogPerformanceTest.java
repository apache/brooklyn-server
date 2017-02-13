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
package org.apache.brooklyn.camp.brooklyn.qa.performance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.core.test.qa.performance.AbstractPerformanceTest;
import org.apache.brooklyn.test.performance.PerformanceMeasurer;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.apache.brooklyn.test.performance.PerformanceTestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class CatalogPerformanceTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogPerformanceTest.class);
    
    private static String TEST_VERSION = "0.1.2";

    protected PerformanceTestResult measure(PerformanceTestDescriptor options) {
        PerformanceTestResult result = PerformanceMeasurer.run(options);
        LOG.info("test="+options+"; result="+result);
        return result;
    }

    protected int numIterations() {
        return 100;
    }
    
    private List<CatalogItem<?, ?>> addItems(int idSuffix) {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: myentity-" + idSuffix,
                "    itemType: entity",
                "    name: My Catalog Entity " + idSuffix,
                "    description: My description",
                "    icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "    item:",
                "      type: " + TestEntity.class.getName(),
                "  - id: myapp-" + idSuffix,
                "    itemType: template",
                "    name: My Catalog App " + idSuffix,
                "    description: My description",
                "    icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "    item:",
                "      services:",
                "      - type: " + TestEntity.class.getName(),
                "  - id: mylocation-" + idSuffix,
                "    itemType: location",
                "    name: My Catalog Location " + idSuffix,
                "    description: My description",
                "    item:",
                "      type: localhost",
                "  - id: mypolicy-" + idSuffix,
                "    itemType: policy",
                "    name: My Catalog Policy " + idSuffix,
                "    description: My description",
                "    item:",
                "      type: " + TestPolicy.class.getName());
        return ImmutableList.copyOf(mgmt().getCatalog().addItems(yaml, false));
    }
    
    @Test(groups={"Integration"})
    public void testAddItems() {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicReference<List<CatalogItem<?,?>>> items = new AtomicReference<>();
        
        Runnable preJob = null;
        
        Runnable job = new Runnable() {
            @Override
            public void run() {
                int i = counter.getAndIncrement();
                items.set(addItems(i));
            }
        };
        Runnable postJob = new Runnable() {
            @Override
            public void run() {
                if (items.get() != null) {
                    for (CatalogItem<?, ?> item : items.get()) {
                        mgmt().getCatalog().deleteCatalogItem(item.getSymbolicName(),  item.getVersion());
                    }
                }
            }
        };
        runPerformanceTest("testAddItems", preJob, job, postJob);
    }
    
    @Test(groups={"Integration"})
    public void testPeekSpecs() {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicReference<List<CatalogItem<?,?>>> items = new AtomicReference<>();
        
        Runnable preJob = new Runnable() {
            @Override
            public void run() {
                int i = counter.getAndIncrement();
                items.set(addItems(i));
            }
        };
        Runnable job = new Runnable() {
            @Override
            public void run() {
                for (CatalogItem<?, ?> item : items.get()) {
                    mgmt().getCatalog().peekSpec(item);
                }
            }
        };
        Runnable postJob = new Runnable() {
            @Override
            public void run() {
                if (items.get() != null) {
                    for (CatalogItem<?, ?> item : items.get()) {
                        mgmt().getCatalog().deleteCatalogItem(item.getSymbolicName(),  item.getVersion());
                    }
                }
            }
        };
        runPerformanceTest("testPeekSpecs", preJob, job, postJob);
    }
    
    @Test(groups={"Integration"})
    public void testPeekSameSpecsRepeatedly() {
        final List<CatalogItem<?, ?>> items = addItems(0);
        
        Runnable job = new Runnable() {
            @Override
            public void run() {
                for (CatalogItem<?, ?> item : items) {
                    mgmt().getCatalog().peekSpec(item);
                }
            }
        };
        runPerformanceTest("testPeekSameSpecsRepeatedly", null, job, null);
    }
    
    protected void runPerformanceTest(String methodName, Runnable preJob, Runnable job, Runnable postJob) {
        int numIterations = numIterations();
        double minRatePerSec = 10 * AbstractPerformanceTest.PERFORMANCE_EXPECTATION;
        
        measure(PerformanceTestDescriptor.create()
                .summary("CatalogPerformanceTest." + methodName)
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .preJob(preJob)
                .job(job)
                .postJob(postJob));
    }
}
