/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.test.framework;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

public class TestEndpointReachableTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TestEndpointReachableTest.class);

    private LocalhostMachineProvisioningLocation loc;
    private ServerSocket serverSocket;
    private HostAndPort serverSocketHostAndPort;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.newLocalhostProvisioningLocation();
        serverSocket = openServerPort();
        serverSocketHostAndPort = toHostAndPort(serverSocket);
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            // keep going; stop the other server sockets etc.
            LOG.warn("Error closing server socket "+serverSocket+"; continuing", e);
        }
        super.tearDown();
    }

    @Test
    public void testHardcodedEndpointReachable() throws Exception {
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, serverSocketHostAndPort.toString()));

        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSensorHostAndPortStringReachable() throws Exception {
        AttributeSensor<String> sensor = Sensors.newStringSensor("test.reachable.endpoint");
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT_SENSOR, sensor));
        app.sensors().set(sensor, serverSocketHostAndPort.toString());
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSensorHostAndPortReachable() throws Exception {
        AttributeSensor<HostAndPort> sensor = Sensors.newSensor(HostAndPort.class, "test.reachable.endpoint");
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT_SENSOR, sensor));
        app.sensors().set(sensor, serverSocketHostAndPort);
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSensorUriStringReachable() throws Exception {
        String sensorVal = "http://"+serverSocketHostAndPort.getHostText()+":"+serverSocketHostAndPort.getPort();
        AttributeSensor<String> sensor = Sensors.newStringSensor("test.reachable.endpoint");
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT_SENSOR, sensor));
        app.sensors().set(sensor, sensorVal);
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSensorUriReachable() throws Exception {
        URI sensorVal = URI.create("http://"+serverSocketHostAndPort.getHostText()+":"+serverSocketHostAndPort.getPort());
        AttributeSensor<URI> sensor = Sensors.newSensor(URI.class, "test.reachable.endpoint");
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT_SENSOR, sensor));
        app.sensors().set(sensor, sensorVal);
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSensorUrlReachable() throws Exception {
        URL sensorVal = new URL("http://"+serverSocketHostAndPort.getHostText()+":"+serverSocketHostAndPort.getPort());
        AttributeSensor<URL> sensor = Sensors.newSensor(URL.class, "test.reachable.endpoint");
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT_SENSOR, sensor));
        app.sensors().set(sensor, sensorVal);
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testHardcodedEndpointReachableWithExplicitAssertionForReachable() throws Exception {
        app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, serverSocketHostAndPort.toString())
                .configure(TestEndpointReachable.ASSERTIONS, ImmutableMap.of(TestEndpointReachable.REACHABLE_KEY, "true")));

        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testExplicitAssertionForNotReachableWhenReachable() throws Exception {
        TestEndpointReachable test = app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, serverSocketHostAndPort.toString())
                .configure(TestEndpointReachable.TIMEOUT, Duration.millis(100))
                .configure(TestEndpointReachable.ASSERTIONS, ImmutableMap.of(TestEndpointReachable.REACHABLE_KEY, "false")));

        try {
            app.start(ImmutableList.of(loc));
        } catch (Exception e) {
            // It should fail to start
            Exceptions.propagateIfFatal(e);
            LOG.debug("As desired, failed to start app with TestEndpointReachable: "+e.toString());
        }
        EntityAsserts.assertAttributeEqualsEventually(test, TestEndpointReachable.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(test, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
    }

    // Not a normal unit-test, because other processes on shared infrastructure can grab the port between
    // us identifying it is free and us checking the reachability.
    @Test(groups="Integration")
    public void testExplicitAssertionForNotReachableWhenNotReachable() throws Exception {
        HostAndPort unusedPort = findUnusedPort();
        TestEndpointReachable test = app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, unusedPort.toString())
                .configure(TestEndpointReachable.ASSERTIONS, ImmutableMap.of(TestEndpointReachable.REACHABLE_KEY, "false")));

        app.start(ImmutableList.of(loc));
        EntityAsserts.assertAttributeEqualsEventually(test, TestEndpointReachable.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(test, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
    }

    // Not a normal unit-test, because other processes on shared infrastructure can grab the port between
    // us identifying it is free and us checking the reachability.
    @Test(groups="Integration")
    public void testHardcodedEndpointNotReachable() throws Exception {
        HostAndPort unusedPort = findUnusedPort();
        TestEndpointReachable test = app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, unusedPort.toString())
                .configure(TestEndpointReachable.TIMEOUT, Duration.millis(100)));

        try {
            app.start(ImmutableList.of(loc));
        } catch (Exception e) {
            // It should fail to start
            Exceptions.propagateIfFatal(e);
            LOG.debug("As desired, failed to start app with TestEndpointReachable: "+e.toString());
        }
        EntityAsserts.assertAttributeEqualsEventually(test, TestEndpointReachable.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(test, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
    }

    // Not a normal unit-test, because other processes on shared infrastructure can grab the port between
    // us identifying it is free and us checking the reachability.
    @Test(groups="Integration")
    public void testSensorEndpointNotReachable() throws Exception {
        AttributeSensor<String> sensor = Sensors.newStringSensor("test.reachable.endpoint");
        HostAndPort unusedPort = findUnusedPort();
        TestEndpointReachable test = app.createAndManageChild(EntitySpec.create(TestEndpointReachable.class)
                .configure(TestEndpointReachable.TARGET_ENTITY, app)
                .configure(TestEndpointReachable.ENDPOINT, unusedPort.toString())
                .configure(TestEndpointReachable.TIMEOUT, Duration.millis(100)));
        app.sensors().set(sensor, unusedPort.toString());
        
        try {
            app.start(ImmutableList.of(loc));
        } catch (Exception e) {
            // It should fail to start
            Exceptions.propagateIfFatal(e);
            LOG.debug("As desired, failed to start app with TestEndpointReachable: "+e.toString());
        }
        EntityAsserts.assertAttributeEqualsEventually(test, TestEndpointReachable.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(test, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
    }

    protected ServerSocket openServerPort() throws IOException {
        InetAddress localAddress = Networking.getReachableLocalHost();
        return new ServerSocket(0, 1024, localAddress);
    }

    protected HostAndPort findUnusedPort() {
        int startPort = 58767;
        int endPort = 60000;
        int port = startPort;
        InetAddress localAddress = Networking.getReachableLocalHost();
        do {
            if (Networking.isPortAvailable(localAddress, port)) {
                return HostAndPort.fromParts(localAddress.getHostAddress(), port);
            }
            port++;
        } while (port <= endPort);
        Assert.fail("Could not find available port in range "+startPort+"-"+endPort);
        throw new IllegalStateException("Impossible code to reach");
    }

    protected HostAndPort toHostAndPort(ServerSocket ss) {
        return HostAndPort.fromParts(serverSocket.getInetAddress().getHostAddress(), serverSocket.getLocalPort());
    }
}
