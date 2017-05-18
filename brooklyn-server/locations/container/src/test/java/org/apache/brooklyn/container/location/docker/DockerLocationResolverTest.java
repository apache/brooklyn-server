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
package org.apache.brooklyn.container.location.docker;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DockerLocationResolverTest extends BrooklynMgmtUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DockerLocationResolverTest.class);

    private BrooklynProperties brooklynProperties;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        brooklynProperties = mgmt.getBrooklynProperties();

        brooklynProperties.put("brooklyn.location.docker.identity", "docker-id");
        brooklynProperties.put("brooklyn.location.docker.credential", "docker-cred");
    }

    @Test
    public void testGivesCorrectLocationType() {
        LocationSpec<?> spec = getLocationSpec("docker");
        assertEquals(spec.getType(), DockerJcloudsLocation.class);

        DockerJcloudsLocation loc = resolve("docker");
        assertTrue(loc instanceof DockerJcloudsLocation, "loc=" + loc);
    }

    @Test
    public void testParametersInSpecString() {
        DockerJcloudsLocation loc = resolve("docker(loginUser=myLoginUser,imageId=myImageId)");
        assertEquals(loc.getConfig(DockerJcloudsLocation.LOGIN_USER), "myLoginUser");
        assertEquals(loc.getConfig(DockerJcloudsLocation.IMAGE_ID), "myImageId");
    }

    @Test
    public void testTakesDotSeparateProperty() {
        brooklynProperties.put("brooklyn.location.docker.loginUser", "myLoginUser");
        DockerJcloudsLocation loc = resolve("docker");
        assertEquals(loc.getConfig(DockerJcloudsLocation.LOGIN_USER), "myLoginUser");
    }

    @Test
    public void testPropertiesPrecedence() {
        // prefer those in "spec" over everything else
        brooklynProperties.put("brooklyn.location.named.mydocker", "docker:(loginUser=\"loginUser-inSpec\")");

        brooklynProperties.put("brooklyn.location.named.mydocker.loginUser", "loginUser-inNamed");
        brooklynProperties.put("brooklyn.location.docker.loginUser", "loginUser-inDocker");
        brooklynProperties.put("brooklyn.location.jclouds.docker.loginUser", "loginUser-inJcloudsProviderSpecific");
        brooklynProperties.put("brooklyn.location.jclouds.loginUser", "loginUser-inJcloudsGeneric");

        // prefer those in "named" over everything else
        brooklynProperties.put("brooklyn.location.named.mydocker.privateKeyFile", "privateKeyFile-inNamed");
        brooklynProperties.put("brooklyn.location.docker.privateKeyFile", "privateKeyFile-inDocker");
        brooklynProperties.put("brooklyn.location.jclouds.docker.privateKeyFile", "privateKeyFile-inJcloudsProviderSpecific");
        brooklynProperties.put("brooklyn.location.jclouds.privateKeyFile", "privateKeyFile-inJcloudsGeneric");

        // prefer those in docker-specific
        brooklynProperties.put("brooklyn.location.docker.publicKeyFile", "publicKeyFile-inDocker");
        brooklynProperties.put("brooklyn.location.jclouds.docker.publicKeyFile", "publicKeyFile-inJcloudsProviderSpecific");
        brooklynProperties.put("brooklyn.location.jclouds.publicKeyFile", "publicKeyFile-inJcloudsGeneric");

        // prefer those in jclouds provider-specific
        brooklynProperties.put("brooklyn.location.jclouds.docker.privateKeyPassphrase", "privateKeyPassphrase-inJcloudsProviderSpecific");
        brooklynProperties.put("brooklyn.location.jclouds.privateKeyPassphrase", "privateKeyPassphrase-inJcloudsGeneric");

        // accept those in jclouds generic
        brooklynProperties.put("brooklyn.location.jclouds.privateKeyData", "privateKeyData-inJcloudsGeneric");

        Map<String, Object> conf = resolve("named:mydocker").config().getBag().getAllConfig();

        assertEquals(conf.get("loginUser"), "loginUser-inSpec");
        assertEquals(conf.get("privateKeyFile"), "privateKeyFile-inNamed");
        assertEquals(conf.get("publicKeyFile"), "publicKeyFile-inDocker");
        assertEquals(conf.get("privateKeyPassphrase"), "privateKeyPassphrase-inJcloudsProviderSpecific");
        assertEquals(conf.get("privateKeyData"), "privateKeyData-inJcloudsGeneric");
    }

    private LocationSpec<?> getLocationSpec(String spec) {
        return mgmt.getLocationRegistry().getLocationSpec(spec).get();
    }

    private DockerJcloudsLocation resolve(String spec) {
        return (DockerJcloudsLocation) mgmt.getLocationRegistry().getLocationManaged(spec);
    }
}
