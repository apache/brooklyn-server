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

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationLiveTest.CREDENTIAL;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationLiveTest.IDENTITY;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationLiveTest.KUBERNETES_ENDPOINT;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEquals;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEventually;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEventuallyNonNull;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertEntityHealthy;
import static org.apache.brooklyn.test.Asserts.succeedsEventually;
import static org.apache.brooklyn.util.http.HttpAsserts.assertHttpStatusCodeEventuallyEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.container.entity.docker.DockerContainer;
import org.apache.brooklyn.container.entity.kubernetes.KubernetesPod;
import org.apache.brooklyn.container.entity.kubernetes.KubernetesResource;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.location.access.PortForwardManagerImpl;
import org.apache.brooklyn.core.network.OnPublicNetworkEnricher;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Live tests for deploying simple blueprints. Particularly useful during dev, but not so useful
 * after that (because assumes the existence of a kubernetes endpoint). It needs configured with
 * something like:
 * <p>
 * {@code -Dtest.brooklyn-container-service.kubernetes.endpoint=http://10.104.2.206:8080}
 */
public class KubernetesLocationYamlLiveTest extends AbstractYamlTest {

    protected KubernetesLocation loc;
    protected List<MachineLocation> machines;
    protected String locationYaml;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        String config = "    {}";

        if (Strings.isNonBlank(KUBERNETES_ENDPOINT)) {
            // if the above is set, we use it.  otherwise use ~/.kube/config
            config = Joiner.on("\n").join(
                    "    " + KubernetesLocationConfig.MASTER_URL.getName() + ": \"" + KUBERNETES_ENDPOINT + "\"",
                    "    " + (StringUtils.isBlank(IDENTITY) ? "" : "identity: " + IDENTITY),
                    "    " + (StringUtils.isBlank(CREDENTIAL) ? "" : "credential: " + CREDENTIAL));
        }
        
        locationYaml = Joiner.on("\n").join(
                "location:",
                "  kubernetes:",
                config);
    }

    @Test(groups = {"Live"})
    public void testLoginPasswordOverride() throws Exception {
        String customPassword = "myDifferentPassword";

        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + EmptySoftwareProcess.class.getName(),
                "    brooklyn.config:",
                "      provisioning.properties:",
                "        " + KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName() + ": " + customPassword);

        Entity app = createStartWaitAndLogApplication(yaml);
        EmptySoftwareProcess entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, EmptySoftwareProcess.class));

        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        assertEquals(machine.config().get(SshMachineLocation.PASSWORD), customPassword);
        assertTrue(machine.isSshable());
    }

    @Test(groups = {"Live"})
    public void testNetcatServer() throws Exception {
        // Runs as root user (hence not `sudo yum install ...`)
        // breaks if shell.env uses attributeWhenReady, so not doing that - see testNetcatServerWithDslInShellEnv()
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + VanillaSoftwareProcess.class.getName(),
                "    brooklyn.parameters:",
                "      - name: netcat.port",
                "        type: port",
                "        default: 8081",
                "    brooklyn.config:",
                "      install.command: |",
                "        yum install -y nc",
                "      launch.command: |",
                "        echo $MESSAGE | nc -l $NETCAT_PORT &",
                "        echo $! > $PID_FILE",
                "      shell.env:",
                "        MESSAGE: mymessage",
                "        NETCAT_PORT: $brooklyn:attributeWhenReady(\"netcat.port\")",
                "    brooklyn.enrichers:",
                "      - type: " + OnPublicNetworkEnricher.class.getName(),
                "        brooklyn.config:",
                "          " + OnPublicNetworkEnricher.SENSORS.getName() + ":",
                "            - netcat.port");

        Entity app = createStartWaitAndLogApplication(yaml);
        VanillaSoftwareProcess entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, VanillaSoftwareProcess.class));

        String publicMapped = assertAttributeEventuallyNonNull(entity, Sensors.newStringSensor("netcat.endpoint.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);

        assertTrue(Networking.isReachable(publicPort), "publicPort=" + publicPort);
    }

    @Test(groups = {"Live"})
    public void testInterContainerNetworking() throws Exception {
        String message = "mymessage";
        int netcatPort = 8081;

        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + VanillaSoftwareProcess.class.getName(),
                "    name: server1",
                "    brooklyn.parameters:",
                "      - name: netcat.port",
                "        type: port",
                "        default: " + netcatPort,
                "    brooklyn.config:",
                "      install.command: |",
                "        yum install -y nc",
                "      launch.command: |",
                "        echo " + message + " | nc -l " + netcatPort + " > netcat.out &",
                "        echo $! > $PID_FILE",
                "  - type: " + VanillaSoftwareProcess.class.getName(),
                "    name: server2",
                "    brooklyn.config:",
                "      install.command: |",
                "        yum install -y nc",
                "      launch.command: true",
                "      checkRunning.command: true");

        Entity app = createStartWaitAndLogApplication(yaml);
        Dumper.dumpInfo(app);

        Entity server1 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("server1"));
        Entity server2 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("server2"));

        SshMachineLocation machine1 = Machines.findUniqueMachineLocation(server1.getLocations(), SshMachineLocation.class).get();
        SshMachineLocation machine2 = Machines.findUniqueMachineLocation(server2.getLocations(), SshMachineLocation.class).get();

        String addr1 = server1.sensors().get(Attributes.SUBNET_ADDRESS);
        String addr2 = server2.sensors().get(Attributes.SUBNET_ADDRESS);

        // Ping between containers
        int result1 = machine1.execCommands("ping-server2", ImmutableList.of("ping -c 4 " + addr2));
        int result2 = machine2.execCommands("ping-server1", ImmutableList.of("ping -c 4 " + addr1));

        // Reach netcat port from other container
        int result3 = machine2.execCommands("nc-to-server1", ImmutableList.of(
                "echo \"fromServer2\" | nc " + addr1 + " " + netcatPort + " > netcat.out",
                "cat netcat.out",
                "grep " + message + " netcat.out"));

        String errMsg = "result1=" + result1 + "; result2=" + result2 + "; result3=" + result3;
        assertEquals(result1, 0, errMsg);
        assertEquals(result2, 0, errMsg);
        assertEquals(result3, 0, errMsg);
    }

    @Test(groups = {"Live"})
    public void testTomcatPod() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesPod.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts: [ \"8080\" ]");

        runTomcat(yaml, KubernetesPod.class);
    }

    @Test(groups = {"Live"})
    public void testTomcatPodExtras() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesPod.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts: [ \"8080\" ]",
                "      metadata:",
                "        extra: test");

        KubernetesPod entity = runTomcat(yaml, KubernetesPod.class);

        String namespace = entity.sensors().get(KubernetesPod.KUBERNETES_NAMESPACE);
        String podName = entity.sensors().get(KubernetesPod.KUBERNETES_POD);
        KubernetesClient client = getClient(entity);
        Pod pod = client.pods().inNamespace(namespace).withName(podName).get();
        Map<String, String> labels = pod.getMetadata().getLabels();
        assertTrue(labels.containsKey("extra"));
        assertEquals(labels.get("extra"), "test");
    }

    @Test(groups = {"Live"})
    public void testTomcatContainer() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + DockerContainer.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts: [ \"8080\" ]");

        runTomcat(yaml, DockerContainer.class);
    }

    /**
     * Assumes that the container entity uses port 8080.
     */
    protected <T extends Entity> T runTomcat(String yaml, Class<T> type) throws Exception {
        Entity app = createStartWaitAndLogApplication(yaml);
        T entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, type));

        Dumper.dumpInfo(app);
        String publicMapped = assertAttributeEventuallyNonNull(entity, Sensors.newStringSensor("docker.port.8080.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);

        assertReachableEventually(publicPort);
        assertHttpStatusCodeEventuallyEquals("http://" + publicPort.getHostText() + ":" + publicPort.getPort(), 200);

        return entity;
    }

    @Test(groups = {"Live"})
    public void testWordpressInContainersWithStartableParent() throws Exception {
        // TODO docker.container.inboundPorts doesn't accept list of ints - need to use quotes
        String randomId = Identifiers.makeRandomLowercaseId(4);
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + BasicStartable.class.getName(),
                "    brooklyn.children:",
                "      - type: " + DockerContainer.class.getName(),
                "        id: wordpress-mysql",
                "        name: mysql",
                "        brooklyn.config:",
                "          docker.container.imageName: mysql:5.6",
                "          docker.container.inboundPorts:",
                "            - \"3306\"",
                "          docker.container.environment:",
                "            MYSQL_ROOT_PASSWORD: \"password\"",
                "          provisioning.properties:",
                "            deployment: wordpress-mysql-" + randomId,
                "      - type: " + DockerContainer.class.getName(),
                "        id: wordpress",
                "        name: wordpress",
                "        brooklyn.config:",
                "          docker.container.imageName: wordpress:4-apache",
                "          docker.container.inboundPorts:",
                "            - \"80\"",
                "          docker.container.environment:",
                "            WORDPRESS_DB_HOST: \"wordpress-mysql-" + randomId + "\"",
                "            WORDPRESS_DB_PASSWORD: \"password\"",
                "          provisioning.properties:",
                "            deployment: wordpress-" + randomId);

        runWordpress(yaml, randomId);
    }

    @Test(groups = {"Live"})
    public void testWordpressInPodsWithStartableParent() throws Exception {
        // TODO docker.container.inboundPorts doesn't accept list of ints - need to use quotes
        String randomId = Identifiers.makeRandomLowercaseId(4);
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + BasicStartable.class.getName(),
                "    brooklyn.children:",
                "      - type: " + KubernetesPod.class.getName(),
                "        id: wordpress-mysql",
                "        name: mysql",
                "        brooklyn.config:",
                "          docker.container.imageName: mysql:5.6",
                "          docker.container.inboundPorts:",
                "            - \"3306\"",
                "          docker.container.environment:",
                "            MYSQL_ROOT_PASSWORD: \"password\"",
                "          deployment: wordpress-mysql-" + randomId,
                "      - type: " + KubernetesPod.class.getName(),
                "        id: wordpress",
                "        name: wordpress",
                "        brooklyn.config:",
                "          docker.container.imageName: wordpress:4-apache",
                "          docker.container.inboundPorts:",
                "            - \"80\"",
                "          docker.container.environment:",
                "            WORDPRESS_DB_HOST: \"wordpress-mysql-" + randomId + "\"",
                "            WORDPRESS_DB_PASSWORD: \"password\"",
                "          deployment: wordpress-" + randomId);

        runWordpress(yaml, randomId);
    }

    @Test(groups = {"Live"})
    public void testWordpressInPods() throws Exception {
        // TODO docker.container.inboundPorts doesn't accept list of ints - need to use quotes
        String randomId = Identifiers.makeRandomLowercaseId(4);
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesPod.class.getName(),
                "    id: wordpress-mysql",
                "    name: mysql",
                "    brooklyn.config:",
                "      docker.container.imageName: mysql:5.6",
                "      docker.container.inboundPorts:",
                "        - \"3306\"",
                "      docker.container.environment:",
                "        MYSQL_ROOT_PASSWORD: \"password\"",
                "      deployment: wordpress-mysql-" + randomId,
                "  - type: " + KubernetesPod.class.getName(),
                "    id: wordpress",
                "    name: wordpress",
                "    brooklyn.config:",
                "      docker.container.imageName: wordpress:4-apache",
                "      docker.container.inboundPorts:",
                "        - \"80\"",
                "      docker.container.environment:",
                "        WORDPRESS_DB_HOST: \"wordpress-mysql-" + randomId + "\"",
                "        WORDPRESS_DB_PASSWORD: \"password\"",
                "      deployment: wordpress-" + randomId);

        runWordpress(yaml, randomId);
    }

    /**
     * Assumes that the {@link DockerContainer} entities have display names of "mysql" and "wordpress",
     * and that they use ports 3306 and 80 respectively.
     */
    protected void runWordpress(String yaml, String randomId) throws Exception {
        Entity app = createStartWaitAndLogApplication(yaml);
        Dumper.dumpInfo(app);

        Iterable<DockerContainer> containers = Entities.descendantsAndSelf(app, DockerContainer.class);
        DockerContainer mysql = Iterables.find(containers, EntityPredicates.displayNameEqualTo("mysql"));
        DockerContainer wordpress = Iterables.find(containers, EntityPredicates.displayNameEqualTo("wordpress"));

        String mysqlPublicPort = assertAttributeEventuallyNonNull(mysql, Sensors.newStringSensor("docker.port.3306.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(mysqlPublicPort));
        assertAttributeEquals(mysql, KubernetesPod.KUBERNETES_NAMESPACE, "brooklyn");
        assertAttributeEquals(mysql, KubernetesPod.KUBERNETES_SERVICE, "wordpress-mysql-" + randomId);

        String wordpressPublicPort = assertAttributeEventuallyNonNull(wordpress, Sensors.newStringSensor("docker.port.80.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(wordpressPublicPort));
        assertAttributeEquals(wordpress, KubernetesPod.KUBERNETES_NAMESPACE, "brooklyn");
        assertAttributeEquals(wordpress, KubernetesPod.KUBERNETES_SERVICE, "wordpress-" + randomId);

        // TODO more assertions (e.g. wordpress can successfully reach the database)
    }

    @Test(groups = {"Live"})
    public void testPod() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesPod.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts:",
                "        - \"8080\"",
                "      shell.env:",
                "        CLUSTER_ID: \"id\"",
                "        CLUSTER_TOKEN: \"token\"");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkPod(app, KubernetesPod.class);
    }

    @Test(groups = {"Live"}, enabled = false)
    public void testPodCatalogEntry() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: kubernetes-pod-entity",
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts:",
                "        - \"8080\"",
                "      shell.env:",
                "        CLUSTER_ID: \"id\"",
                "        CLUSTER_TOKEN: \"token\"");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkPod(app, KubernetesPod.class);
    }

    protected <T extends Entity> void checkPod(Entity app, Class<T> type) {
        T container = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, type));

        Dumper.dumpInfo(app);

        String publicMapped = assertAttributeEventuallyNonNull(container, Sensors.newStringSensor("docker.port.8080.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);

        assertReachableEventually(publicPort);
        assertHttpStatusCodeEventuallyEquals("http://" + publicPort.getHostText() + ":" + publicPort.getPort(), 200);
    }

    @Test(groups = {"Live"})
    public void testNginxReplicationController() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesResource.class.getName(),
                "    id: nginx-replication-controller",
                "    name: \"nginx-replication-controller\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx-replication-controller.yaml");

        Entity app = createStartWaitAndLogApplication(yaml);
        checkNginxResource(app, KubernetesResource.class);
    }

    protected <T extends Entity> void checkNginxResource(Entity app, Class<T> type) {
        T entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, type));

        Dumper.dumpInfo(app);

        assertEntityHealthy(entity);
        assertAttributeEqualsEventually(entity, KubernetesResource.RESOURCE_NAME, "nginx-replication-controller");
        assertAttributeEqualsEventually(entity, KubernetesResource.RESOURCE_TYPE, "ReplicationController");
        assertAttributeEqualsEventually(entity, KubernetesResource.KUBERNETES_NAMESPACE, "default");
        assertAttributeEventually(entity, SoftwareProcess.ADDRESS, and(notNull(), not(equalTo("0.0.0.0"))));
        assertAttributeEventually(entity, SoftwareProcess.SUBNET_ADDRESS, and(notNull(), not(equalTo("0.0.0.0"))));
    }

    @Test(groups = {"Live"})
    public void testNginxService() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesResource.class.getName(),
                "    id: nginx-replication-controller",
                "    name: \"nginx-replication-controller\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx-replication-controller.yaml",
                "  - type: " + KubernetesResource.class.getName(),
                "    id: nginx-service",
                "    name: \"nginx-service\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx-service.yaml");
        Entity app = createStartWaitAndLogApplication(yaml);

        Iterable<KubernetesResource> resources = Entities.descendantsAndSelf(app, KubernetesResource.class);
        KubernetesResource nginxReplicationController = Iterables.find(resources, EntityPredicates.displayNameEqualTo("nginx-replication-controller"));
        KubernetesResource nginxService = Iterables.find(resources, EntityPredicates.displayNameEqualTo("nginx-service"));

        assertEntityHealthy(nginxReplicationController);
        assertEntityHealthy(nginxService);

        Dumper.dumpInfo(app);

        Integer httpPort = assertAttributeEventuallyNonNull(nginxService, Sensors.newIntegerSensor("kubernetes.http.port"));
        assertEquals(httpPort, Integer.valueOf(80));
        String httpPublicPort = assertAttributeEventuallyNonNull(nginxService, Sensors.newStringSensor("kubernetes.http.endpoint.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(httpPublicPort));
    }
    
    @Test(groups = {"Live"})
    public void testNginxService2() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "  - type: " + KubernetesResource.class.getName(),
                "    name: \"nginx-2-deployment\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx-2-deployment.yaml",
                "  - type: " + KubernetesResource.class.getName(),
                "    name: \"nginx-2-service\"",
                "    brooklyn.config:",
                "      resource: classpath://nginx-2-service.yaml");
        Entity app = createStartWaitAndLogApplication(yaml);

        Iterable<KubernetesResource> resources = Entities.descendantsAndSelf(app, KubernetesResource.class);
        KubernetesResource nginxDeployment = Iterables.find(resources, EntityPredicates.displayNameEqualTo("nginx-2-deployment"));
        KubernetesResource nginxService = Iterables.find(resources, EntityPredicates.displayNameEqualTo("nginx-2-service"));

        assertEntityHealthy(nginxDeployment);
        assertEntityHealthy(nginxService);

        Dumper.dumpInfo(app);

        Integer httpPort = assertAttributeEventuallyNonNull(nginxService, Sensors.newIntegerSensor("kubernetes.http.port"));
        assertEquals(httpPort, Integer.valueOf(80));
        String httpPublicPort = assertAttributeEventuallyNonNull(nginxService, Sensors.newStringSensor("kubernetes.http.endpoint.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(httpPublicPort));
        
        // also check locations are cleaned up
        mgmt().getApplications().forEach(appI -> ((StartableApplication)appI).stop());
        Collection<Location> ll = mgmt().getLocationManager().getLocations();
        if (ll.isEmpty()) {
            // okay
        } else {
            // should have an empty PortForwardingManager
            Asserts.assertSize(ll, 1);  
            Asserts.assertSize(((PortForwardManagerImpl)Iterables.getOnlyElement(ll)).getPortMappings(), 0);
        }
    }
    
    protected void assertReachableEventually(final HostAndPort hostAndPort) {
        succeedsEventually(new Runnable() {
            public void run() {
                assertTrue(Networking.isReachable(hostAndPort), "publicPort=" + hostAndPort);
            }
        });
    }

    public KubernetesClient getClient(Entity entity) {
        MachineProvisioningLocation<?> location = entity.sensors().get(SoftwareProcess.PROVISIONING_LOCATION);
        if (location instanceof KubernetesLocation) {
            KubernetesLocation kubernetes = (KubernetesLocation) location;
            ConfigBag config = kubernetes.config().getBag();
            KubernetesClientRegistry registry = kubernetes.config().get(KubernetesLocationConfig.KUBERNETES_CLIENT_REGISTRY);
            KubernetesClient client = registry.getKubernetesClient(config);
            return client;
        }
        throw new IllegalStateException("Cannot find KubernetesLocation on entity: " + Iterables.toString(entity.getLocations()));
    }
}
