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
package org.apache.brooklyn.entity.software.base;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterInMemorySizeIntegrationTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

@Test
public class SoftwareProcessPersisterInMemorySizeIntegrationTest extends BrooklynMementoPersisterInMemorySizeIntegrationTest {

    public SoftwareProcessPersisterInMemorySizeIntegrationTest() {
        pass1MaxFiles = 80;
        pass1MaxKb = 200;
        pass2MaxFiles = 100;
        pass2MaxKb = 160;
        pass3MaxKb = 50;
    }

    @Override
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        app.createAndManageChild(EntitySpec.create(DoNothingSoftwareProcess.class)
                .configure(SoftwareProcess.LIFECYCLE_EFFECTOR_TASKS, new SoftwareProcessDriverLifecycleEffectorTasks()));
        app.start(ImmutableList.of(location));
    }

    // to allow selection for running in IDE
    public void testPersistenceVolumeFast() throws Exception {
        super.testPersistenceVolumeFast();
    }

}
