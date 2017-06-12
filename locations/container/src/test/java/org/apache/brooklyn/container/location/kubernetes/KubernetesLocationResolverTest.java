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
package org.apache.brooklyn.container.location.kubernetes;

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

public class KubernetesLocationResolverTest extends BrooklynMgmtUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocationResolverTest.class);

    private BrooklynProperties brooklynProperties;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        brooklynProperties = mgmt.getBrooklynProperties();

        brooklynProperties.put("brooklyn.location.kubernetes.identity", "kubernetes-id");
        brooklynProperties.put("brooklyn.location.kubernetes.credential", "kubernetes-cred");
    }

    @Test
    public void testGivesCorrectLocationType() {
        LocationSpec<?> spec = getLocationSpec("kubernetes");
        assertEquals(spec.getType(), KubernetesLocation.class);

        KubernetesLocation loc = resolve("kubernetes");
        assertTrue(loc instanceof KubernetesLocation, "loc=" + loc);
    }

    @Test
    public void testParametersInSpecString() {
        KubernetesLocation loc = resolve("kubernetes(endpoint=myMasterUrl)");
        assertEquals(loc.getConfig(KubernetesLocation.MASTER_URL), "myMasterUrl");
    }

    @Test
    public void testTakesDotSeparateProperty() {
        brooklynProperties.put("brooklyn.location.kubernetes.endpoint", "myMasterUrl");
        KubernetesLocation loc = resolve("kubernetes");
        assertEquals(loc.getConfig(KubernetesLocation.MASTER_URL), "myMasterUrl");
    }

    @Test
    public void testPropertiesPrecedence() {
        // prefer those in "spec" over everything else
        brooklynProperties.put("brooklyn.location.named.mykubernetes", "kubernetes:(loginUser=\"loginUser-inSpec\")");

        brooklynProperties.put("brooklyn.location.named.mykubernetes.loginUser", "loginUser-inNamed");
        brooklynProperties.put("brooklyn.location.kubernetes.loginUser", "loginUser-inDocker");

        // prefer those in "named" over everything else
        brooklynProperties.put("brooklyn.location.named.mykubernetes.privateKeyFile", "privateKeyFile-inNamed");
        brooklynProperties.put("brooklyn.location.kubernetes.privateKeyFile", "privateKeyFile-inDocker");

        // prefer those in kubernetes-specific
        brooklynProperties.put("brooklyn.location.kubernetes.publicKeyFile", "publicKeyFile-inDocker");

        Map<String, Object> conf = resolve("named:mykubernetes").config().getBag().getAllConfig();

        assertEquals(conf.get("loginUser"), "loginUser-inSpec");
        assertEquals(conf.get("privateKeyFile"), "privateKeyFile-inNamed");
        assertEquals(conf.get("publicKeyFile"), "publicKeyFile-inDocker");
    }

    private LocationSpec<?> getLocationSpec(String spec) {
        LOG.debug("Obtaining location spec '{}'", spec);
        return mgmt.getLocationRegistry().getLocationSpec(spec).get();
    }

    private KubernetesLocation resolve(String spec) {
        LOG.debug("Resolving location spec '{}'", spec);
        return (KubernetesLocation) mgmt.getLocationRegistry().getLocationManaged(spec);
    }
}
