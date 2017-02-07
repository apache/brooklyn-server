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
package org.apache.brooklyn.entity;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.test.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class SimpleAppOpenstackLiveTest extends AbstractOpenstackLiveTest {

    private static Logger LOG = LoggerFactory.getLogger(SimpleAppOpenstackLiveTest.class);

    public void test_Centos_6() throws Exception { }

    @Override
    protected void doTest(Location loc) throws Exception {

        LOG.info("Testing in {}", loc);
        final EmptySoftwareProcess server = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class));
        app.start(ImmutableList.of(loc));

        LOG.info("App started, waiting for RUNNING");
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                EntityAsserts.assertAttributeEqualsEventually(server, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            }});
    }
}
