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
package org.apache.brooklyn.container.location.openshift;

import static org.apache.brooklyn.container.location.openshift.OpenShiftLocationLiveTest.CA_CERT_FILE;
import static org.apache.brooklyn.container.location.openshift.OpenShiftLocationLiveTest.CLIENT_CERT_FILE;
import static org.apache.brooklyn.container.location.openshift.OpenShiftLocationLiveTest.CLIENT_KEY_FILE;
import static org.apache.brooklyn.container.location.openshift.OpenShiftLocationLiveTest.NAMESPACE;
import static org.apache.brooklyn.container.location.openshift.OpenShiftLocationLiveTest.OPENSHIFT_ENDPOINT;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.container.entity.openshift.OpenShiftPod;
import org.apache.brooklyn.container.entity.openshift.OpenShiftResource;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocationYamlLiveTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;

/**
 * Tests YAML apps via the {@code openshift} location, to an OpenShift endpoint.
 * By extending {@link KubernetesLocationYamlLiveTest}, we get all the k8s tests.
 * <p>
 * It needs configured with something like:
 * <p>
 * <pre>{@code
 * -Dtest.brooklyn-container-service.openshift.endpoint=https://master.example.com:8443/
 * -Dtest.brooklyn-container-service.openshift.certsBaseDir=/Users/aled/repos/grkvlt/40bdf09b09d5896e19a9d287f41d39bb
 * -Dtest.brooklyn-container-service.openshift.namespace=test
 * }</pre>
 */
public class OpenShiftLocationYamlLiveTest extends KubernetesLocationYamlLiveTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        locationYaml = Joiner.on("\n").join(
                "location:",
                "  openshift:",
                "    " + OpenShiftLocation.CLOUD_ENDPOINT.getName() + ": \"" + OPENSHIFT_ENDPOINT + "\"",
                "    " + OpenShiftLocation.CA_CERT_FILE.getName() + ": \"" + CA_CERT_FILE + "\"",
                "    " + OpenShiftLocation.CLIENT_CERT_FILE.getName() + ": \"" + CLIENT_CERT_FILE + "\"",
                "    " + OpenShiftLocation.CLIENT_KEY_FILE.getName() + ": \"" + CLIENT_KEY_FILE + "\"",
                "    " + OpenShiftLocation.NAMESPACE.getName() + ": \"" + NAMESPACE + "\"",
                "    " + OpenShiftLocation.PRIVILEGED.getName() + ": true",
                "    " + OpenShiftLocation.LOGIN_USER_PASSWORD.getName() + ": p4ssw0rd");
    }

    @Test(groups = {"Live"})
    public void testTomcatOpenShiftPod() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + OpenShiftPod.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts: [ \"8080\" ]");

        runTomcat(yaml, OpenShiftPod.class);
    }

    @Test(groups = {"Live"})
    public void testOpenShiftPod() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + OpenShiftPod.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts:",
                "        - \"8080\"",
                "      shell.env:",
                "        CLUSTER_ID: \"id\"",
                "        CLUSTER_TOKEN: \"token\"");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkPod(app, OpenShiftPod.class);
    }

    @Test(groups = {"Live"}, enabled = false)
    public void testOpenShiftPodCatalogEntry() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: openshift-pod-entity",
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts:",
                "        - \"8080\"",
                "      shell.env:",
                "        CLUSTER_ID: \"id\"",
                "        CLUSTER_TOKEN: \"token\"");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkPod(app, OpenShiftPod.class);
    }

    @Test(groups = {"Live"})
    public void testNginxOpenShiftResource() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + OpenShiftResource.class.getName(),
                "    id: nginx",
                "    name: \"nginx\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx.yaml");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkNginxResource(app, OpenShiftResource.class);
    }

    @Test(groups = {"Live"}, enabled = false)
    public void testNginxOpenShiftResourceCatalogEntry() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: openshift-resource-entity",
                "    id: nginx",
                "    name: \"nginx\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx.yaml");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkNginxResource(app, OpenShiftResource.class);
    }

}
