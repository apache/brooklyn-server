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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.effector.http.HttpCommandEffector;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class HttpCommandEffectorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(HttpCommandEffectorYamlTest.class);

    @Test(groups="Integration")
    public void testHttpCommandEffectorWithParameters() throws Exception {
        Entity app = createAndStartApplication(
            "location: localhost",
            "services:",
            "- type: " + EmptySoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: forever",
            "  brooklyn.initializers:",
            "  - type: " + HttpCommandEffector.class.getName(),
            "    brooklyn.config:",
            "      name: myEffector",
            "      description: myDescription",
            "      uri: http://httpbin.org/get?id=myId",
            "      httpVerb: GET",
            "      jsonPath: $.args.id",
            "      publishSensor: results"
        );
        waitForApplicationTasks(app);

        EmptySoftwareProcess entity = (EmptySoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myEffector").get();

        // Invoke with parameters
        {
            Object result = entity.invoke(effector, ImmutableMap.of("uri", "http://httpbin.org/get?pwd=passwd", "jsonPath", "$.args.pwd")).get();
            assertEquals(((String)result).trim(), "passwd");

        }

        // Invoke with default parameter
        {
            Object result = entity.invoke(effector, ImmutableMap.<String, Object>of()).get();
            assertEquals(((String)result).trim(), "myId");
        }
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
