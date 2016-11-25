package io.cloudsoft.amp.container.kubernetes.location;

import static io.cloudsoft.amp.container.kubernetes.location.KubernetesLocationLiveTest.CREDENTIAL;
import static io.cloudsoft.amp.container.kubernetes.location.KubernetesLocationLiveTest.IDENTITY;
import static io.cloudsoft.amp.container.kubernetes.location.KubernetesLocationLiveTest.KUBERNETES_ENDPOINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.network.OnPublicNetworkEnricher;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.net.Networking;
import org.apache.logging.log4j.util.Strings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import io.cloudsoft.amp.container.kubernetes.entity.KubernetesPod;
import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer;

/**
 * Live tests for deploying simple blueprints. Particularly useful during dev, but not so useful
 * after that (because assumes the existence of a kubernetes endpoint). It needs configured with 
 * something like:
 * 
 *   {@code =Dtest.amp.kubernetes.endpoint=http://10.104.2.206:8080}).
 * 
 * The QA Framework is more important for that - hence these tests (trying to be) kept simple 
 * and focused.
 */
public class KubernetesLocationYamlLiveTest extends AbstractYamlTest {

    protected KubernetesLocation loc;
    protected List<MachineLocation> machines;
    protected String locationYaml;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        locationYaml = Joiner.on("\n").join(
                "location:",
                "  kubernetes:",
                "    " + KubernetesLocationConfig.CLOUD_ENDPOINT.getName() + ": \"" + KUBERNETES_ENDPOINT + "\"",
                "    " + KubernetesLocationConfig.IMAGE.getName() + ": cloudsoft/centos:7",
                "    " + KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName() + ": p4ssw0rd",
                "    " + (Strings.isBlank(IDENTITY) ? "" : "identity: "+IDENTITY),
                "    " + (Strings.isBlank(CREDENTIAL) ? "" : "credential: "+CREDENTIAL));
    }
    
    @Test(groups={"Live"})
    public void testLoginPasswordOverride() throws Exception {
        String customPassword = "myDifferentPassword";
        
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + EmptySoftwareProcess.class.getName(),
                "  brooklyn.config:",
                "    provisioning.properties:",
                "      " + KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName() + ": " + customPassword,
                "    shell.env:",
                "      CLOUDSOFT_ROOT_PASSWORD: " + customPassword);
        Entity app = createStartWaitAndLogApplication(yaml);
        EmptySoftwareProcess entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, EmptySoftwareProcess.class));
        
        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        assertEquals(machine.config().get(SshMachineLocation.PASSWORD), customPassword);
        assertTrue(machine.isSshable());
    }

    @Test(groups={"Live"})
    public void testNetcatServer() throws Exception {
        // Runs as root user (hence not `sudo yum install ...`)
        // breaks if shell.env uses attributeWhenReady, so not doing that - see testNetcatServerWithDslInShellEnv()
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  brooklyn.parameters:",
                "  - name: netcat.port",
                "    type: port",
                "    default: 8081",
                "  brooklyn.initializers:",
                "  - type: " + StaticSensor.class.getName(),
                "    brooklyn.config:",
                "      name: netcat.port",
                "      targetType: int",
                "      static.value: 8081",
                "  brooklyn.config:",
                "    install.command: |",
                "      yum install -y nc",
                "    launch.command: |",
                "      echo $MESSAGE | nc -l $NETCAT_PORT &",
                "      echo $! > $PID_FILE",
                "    checkRunning.command: |",
                "      true",
                "    shell.env:",
                "      MESSAGE: mymessage",
                "      NETCAT_PORT: 8081",
                "  brooklyn.enrichers:",
                "  - type: " + OnPublicNetworkEnricher.class.getName(),
                "    brooklyn.config:",
                "      " + OnPublicNetworkEnricher.SENSORS.getName() + ":",
                "      - netcat.port");
        Entity app = createStartWaitAndLogApplication(yaml);
        VanillaSoftwareProcess entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, VanillaSoftwareProcess.class));
        
        String publicMapped = EntityAsserts.assertAttributeEventuallyNonNull(entity, Sensors.newStringSensor("netcat.endpoint.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);
        
        assertTrue(Networking.isReachable(publicPort), "publicPort="+publicPort);
    }

    // TODO Fails because kubernetesLocation.obtain() tries to retrieve the 'shell.env' config val,
    // but that value is waiting for a port. The PortAttributeSensorAndConfigKey will only set the
    // sensor once there is a location object set on the entity itself. Hangs forever.
    //
    //     at org.apache.brooklyn.core.entity.AbstractEntity.getConfig(AbstractEntity.java:1279)
    //     at io.cloudsoft.amp.container.kubernetes.location.KubernetesLocation.findEnvironmentVariables(KubernetesLocation.java:492)
    //     at io.cloudsoft.amp.container.kubernetes.location.KubernetesLocation.createKubernetesContainerLocation(KubernetesLocation.java:202)
    //     at io.cloudsoft.amp.container.kubernetes.location.KubernetesLocation.obtain(KubernetesLocation.java:134)
    @Test(groups={"Broken", "Live"}, enabled=false)
    public void testNetcatServerWithDslInShellEnv() throws Exception {
        // Runs as root user (hence not `sudo yum install ...`)
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  brooklyn.parameters:",
                "  - name: netcat.port",
                "    type: port",
                "    default: 8081",
                "  brooklyn.config:",
                "    install.command: |",
                "      yum install -y nc",
                "    launch.command: |",
                "      echo $MESSAGE | nc -l $NETCAT_PORT &",
                "      echo $! > $PID_FILE",
                "    checkRunning.command: |",
                "      true",
                "    shell.env:",
                "      MESSAGE: mymessage",
                "      NETCAT_PORT: $brooklyn:attributeWhenReady(\"netcat.port\")",
                "  brooklyn.enrichers:",
                "  - type: " + OnPublicNetworkEnricher.class.getName(),
                "    brooklyn.config:",
                "      " + OnPublicNetworkEnricher.SENSORS.getName() + ":",
                "      - netcat.port");
        Entity app = createStartWaitAndLogApplication(yaml);
        VanillaSoftwareProcess entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, VanillaSoftwareProcess.class));
        
        String publicMapped = EntityAsserts.assertAttributeEventuallyNonNull(entity, Sensors.newStringSensor("netcat.endpoint.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);
        
        assertTrue(Networking.isReachable(publicPort), "publicPort="+publicPort);
    }

    @Test(groups={"Live"})
    public void testInterContainerNetworking() throws Exception {
        String message = "mymessage";
        int netcatPort = 8081;
        
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  name: server1",
                "  brooklyn.parameters:",
                "  - name: netcat.port",
                "    type: port",
                "    default: " + netcatPort,
                "  brooklyn.config:",
                "    install.command: |",
                "      yum install -y nc",
                "    launch.command: |",
                "      echo " + message + " | nc -l " + netcatPort + " > netcat.out &",
                "      echo $! > $PID_FILE",
                "    checkRunning.command: |",
                "      true",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  name: server2",
                "  brooklyn.config:",
                "    install.command: |",
                "      yum install -y nc",
                "    launch.command: |",
                "      true",
                "    checkRunning.command: |",
                "      true");
        Entity app = createStartWaitAndLogApplication(yaml);
        Entities.dumpInfo(app);
        
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
        
        String errMsg = "result1="+result1+"; result2="+result2+"; result3="+result3;
        assertEquals(result1, 0, errMsg);
        assertEquals(result2, 0, errMsg);
        assertEquals(result3, 0, errMsg);
    }

    @Test(groups={"Live"})
    public void testTomcatContainer() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + DockerContainer.class.getName(),
                "  brooklyn.config:",
                "    docker.container.imageName: tomcat",
                "    docker.container.inboundPorts: [ \"8080\" ]");
        Entity app = createStartWaitAndLogApplication(yaml);
        DockerContainer entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, DockerContainer.class));
        
        Entities.dumpInfo(app);
        String publicMapped = EntityAsserts.assertAttributeEventuallyNonNull(entity, Sensors.newStringSensor("docker.port.8080.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);
        
        assertReachableEventually(publicPort);
        HttpAsserts.assertHttpStatusCodeEventuallyEquals("http://"+publicPort.getHostText()+":"+publicPort.getPort(), 200);
    }

    @Test(groups={"Live"})
    public void testWordpress() throws Exception {
        // TODO docker.container.inboundPorts doesn't accept list of ints - need to use quotes
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + KubernetesPod.class.getName(),
                "  brooklyn.children:",
                "  - type: " + DockerContainer.class.getName(),
                "    id: wordpress-mysql",
                "    name: mysql",
                "    brooklyn.config:",
                "      docker.container.imageName: mysql:5.6",
                "      docker.container.inboundPorts:",
                "      - \"3306\"",
                "      env:",
                "        MYSQL_ROOT_PASSWORD: \"password\"",
                "      provisioning.properties:",
                "        kubernetes.deployment: wordpress-mysql",
                "  - type: " + DockerContainer.class.getName(),
                "    id: wordpress",
                "    name: wordpress",
                "    brooklyn.config:",
                "      docker.container.imageName: wordpress:4.4-apache",
                "      docker.container.inboundPorts:",
                "      - \"80\"",
                "      env:",
                "        WORDPRESS_DB_HOST: \"wordpress-mysql\"",
                "        WORDPRESS_DB_PASSWORD: \"password\"",
                "      provisioning.properties:",
                "        kubernetes.deployment: wordpress");
        Entity app = createStartWaitAndLogApplication(yaml);
        Entities.dumpInfo(app);
        
        Iterable<DockerContainer> containers = Entities.descendantsAndSelf(app, DockerContainer.class);
        DockerContainer mysql = Iterables.find(containers, EntityPredicates.displayNameEqualTo("mysql"));
        DockerContainer wordpress = Iterables.find(containers, EntityPredicates.displayNameEqualTo("wordpress"));
        
        String mysqlPublicPort = EntityAsserts.assertAttributeEventuallyNonNull(mysql, Sensors.newStringSensor("docker.port.3306.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(mysqlPublicPort));
        
        String wordpressPublicPort = EntityAsserts.assertAttributeEventuallyNonNull(wordpress, Sensors.newStringSensor("docker.port.80.mapped.public"));
        assertReachableEventually(HostAndPort.fromString(wordpressPublicPort));
        
        // TODO more assertions (e.g. wordpress can successfully reach the database)
    }

    @Test(groups={"Live"})
    public void testPod() throws Exception {
        String yaml = Joiner.on("\n").join(
                locationYaml,
                "services:",
                "- type: " + KubernetesPod.class.getName(),
                "  brooklyn.children:",
                "  - type: " + DockerContainer.class.getName(),
                "    brooklyn.config:",
                "      docker.container.imageName: tomcat",
                "      docker.container.inboundPorts:",
                "      - \"8080\"",
                "      env:",
                "        CLUSTER_ID: \"id\"",
                "        CLUSTER_TOKEN: \"token\"");
        Entity app = createStartWaitAndLogApplication(yaml);
        DockerContainer container = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, DockerContainer.class));
        
        Entities.dumpInfo(app);
        String publicMapped = EntityAsserts.assertAttributeEventuallyNonNull(container, Sensors.newStringSensor("docker.port.8080.mapped.public"));
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);
        
        assertReachableEventually(publicPort);
        HttpAsserts.assertHttpStatusCodeEventuallyEquals("http://"+publicPort.getHostText()+":"+publicPort.getPort(), 200);
    }
    
    protected void assertReachableEventually(final HostAndPort hostAndPort) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertTrue(Networking.isReachable(hostAndPort), "publicPort="+hostAndPort);
            }});
    }
}
