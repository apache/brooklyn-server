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
package org.apache.brooklyn.test.framework.live;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.location.BasicLocationDefinition;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils.CreationResult;
import org.apache.brooklyn.launcher.SimpleYamlLauncherForTests;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

public abstract class TestFrameworkLiveTest {
    private static final Logger log = LoggerFactory.getLogger(TestFrameworkLiveTest.class);

    private SimpleYamlLauncherForTests launcher;
    private ManagementContext mgmt;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        launcher = new SimpleYamlLauncherForTests();
        mgmt = launcher.getManagementContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (launcher != null) {
            launcher.destroyAll();
        }
    }

    @DataProvider
    public abstract Object[][] tests();

    @Test(dataProvider="tests", groups="Live")
    public void runTestFrameworkTest(TestFrameworkRun testRun) {
        for (Map.Entry<String, String> alias : testRun.locationAliases().entries()) {
            mgmt.getLocationRegistry().updateDefinedLocation(
                    new BasicLocationDefinition(alias.getValue(), alias.getValue(), alias.getKey(), null));
        }

        for (URL prereq : testRun.prerequisites()) {
            addCatalogItems(prereq);
        }
        Collection<String> testIds = new ArrayList<>();
        for (URL test : testRun.tests()) {
            testIds.addAll(addCatalogItems(test));
        }
        Collection<CreationResult<? extends Application, Void>> tests = new ArrayList<>();
        for (String testId : testIds) {
            String testYaml = yaml(
                    "location: " + testRun.location(),
                    "services:",
                    "- type: "+ testId);
            CreationResult<? extends Application, Void> result = EntityManagementUtils.createStarting(mgmt, testYaml);
            tests.add(result);
        }
        for (CreationResult<? extends Application, Void> t : tests) {
            t.blockUntilComplete();
        }
        
        boolean isInteractive = Boolean.getBoolean("test.framework.interactive");
        if (isInteractive) {
            boolean hasFailed = false;
            for (CreationResult<? extends Application, Void> t : tests) {
                hasFailed = hasFailed || t.task().isError();
            }
            if (hasFailed) {
                log.error("The test failed, blocking until user unmanages all entities.");
                while (!mgmt.getApplications().isEmpty()) {
                    Duration.sleep(Duration.ONE_SECOND);
                }
            }
        }

        for (CreationResult<? extends Application, Void> t : tests) {
            t.task().getUnchecked();
        }
    }

    private String yaml(String... lines) {
        return Joiner.on("\n").join(lines);
    }

    private Collection<String> addCatalogItems(URL url) {
        try {
            return addCatalogItems(Streams.readFullyStringAndClose(url.openStream()));
        } catch (IOException e) {
            throw Exceptions.propagate("Failed to load url " + url, e);
        }
    }

    private Collection<String> addCatalogItems(String yaml) {
        return FluentIterable.from(mgmt.getCatalog().addItems(yaml))
                .transform(item -> item.getId())
                .toList();
    }
}
