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
package io.cloudsoft.amp.container.openshift.location;

import org.apache.brooklyn.launcher.blueprints.AbstractBlueprintTest;
import org.testng.annotations.Test;

public class SimpleBlueprintsLiveTest extends AbstractBlueprintTest {

    @Test(groups={"Live"})
    public void testSimpleServer() throws Exception {
        runTest("blueprints/simple-server-on-openshift.yaml");
    }

    @Test(groups={"Live"})
    public void testVanillaSoftwareProcess() throws Exception {
        runTest("blueprints/vanilla-software-process-on-openshift.yaml");
    }

    @Test(groups={"Live"})
    public void testTomcatDockerImage() throws Exception {
        runTest("blueprints/tomcat-docker-image-on-openshift.yaml");
    }

    @Test(groups={"Live"})
    public void testWordpressDockerImage() throws Exception {
        runTest("blueprints/mysql_wordpress-docker-images-on-openshift.yaml");
    }

    @Test(groups={"Live"}, enabled = false)
    public void testIronRunnerDockerImage() throws Exception {
        runTest("blueprints/iron_runner-docker-image-on-openshift.yaml");
    }
}
