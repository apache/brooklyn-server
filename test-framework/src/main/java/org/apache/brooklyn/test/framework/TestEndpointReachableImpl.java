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

import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAssertions;

import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.TestFrameworkAssertions.AssertionSupport;
import org.apache.brooklyn.test.framework.TestHttpCall.HttpAssertionTarget;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * {@inheritDoc}
 */
public class TestEndpointReachableImpl extends TargetableTestComponentImpl implements TestEndpointReachable {

    private static final Logger LOG = LoggerFactory.getLogger(TestEndpointReachableImpl.class);

    @Override
    public void start(Collection<? extends Location> locations) {
        if (!getChildren().isEmpty()) {
            throw new RuntimeException(String.format("The entity [%s] cannot have child entities", getClass().getName()));
        }
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        final String endpoint = getConfig(ENDPOINT);
        final Object endpointSensor = getConfig(ENDPOINT_SENSOR);
        final Duration timeout = getConfig(TIMEOUT);
        final Entity target = resolveTarget();

        if (endpoint == null && endpointSensor == null) {
            throw new RuntimeException(String.format("The entity [%s] must be configured with one of endpoint or endpointSensor", getClass().getName()));
        } else if (endpoint != null && endpointSensor != null) {
            throw new RuntimeException(String.format("The entity [%s] must be configured with only one of endpoint or endpointSensor", getClass().getName()));
        }
        
        final Supplier<HostAndPort> supplier = new Supplier<HostAndPort>() {
            @Override
            public HostAndPort get() {
                Object val;
                if (endpoint != null) {
                    val = endpoint;
                } else if (endpointSensor instanceof AttributeSensor) {
                    val = target.sensors().get((AttributeSensor<?>)endpointSensor);
                } else if (endpointSensor instanceof CharSequence) {
                    AttributeSensor<Object> sensor = Sensors.newSensor(Object.class, ((CharSequence)endpointSensor).toString());
                    val = target.sensors().get(sensor);
                } else {
                    throw new IllegalArgumentException(String.format("The entity [%s] has endpointSensor of invalid type %s [%s]", getClass().getName(), endpointSensor.getClass().getName(), endpointSensor));
                }
                return (val == null) ? null : toHostAndPort(val);
            }
        };
        if (endpoint != null) {
            // fail-fast if have a static endpoint value
            supplier.get();
        }

        try {
            Asserts.succeedsEventually(ImmutableMap.of("timeout", timeout), new Runnable() {
                @Override
                public void run() {
                    HostAndPort val = supplier.get();
                    Asserts.assertNotNull(val);
                    Asserts.assertTrue(Networking.isReachable(val), val+" not reachable");
                }});
            sensors().set(Attributes.SERVICE_UP, true);
            ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);

        } catch (Throwable t) {
            LOG.info("{} [{}] test failed", this, endpoint != null ? endpoint : endpointSensor);
            sensors().set(Attributes.SERVICE_UP, false);
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    protected HostAndPort toHostAndPort(Object endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException(String.format("The entity [%s] has no endpoint", getClass().getName()));
        } else if (endpoint instanceof String) {
            return toHostAndPort((String)endpoint);
        } else if (endpoint instanceof URI) {
            return toHostAndPort(((URI)endpoint).toString());
        } else if (endpoint instanceof URL) {
            return toHostAndPort(((URL)endpoint).toString());
        } else if (endpoint instanceof HostAndPort) {
            return (HostAndPort)endpoint;
        } else {
            throw new IllegalArgumentException(String.format("The entity [%s] has endpoint of invalid type %s [%s]", getClass().getName(), endpoint.getClass().getName(), endpoint));
        }
    }
    
    protected HostAndPort toHostAndPort(String endpoint) {
        if (Strings.isEmpty(endpoint)) {
            throw new IllegalArgumentException(String.format("The entity [%s] has no endpoint", getClass().getName()));
        }
        try {
            URI uri = URI.create(endpoint);
            int port;
            if (uri.getPort() != -1) {
                port = uri.getPort();
            } else {
                if ("http".equalsIgnoreCase(uri.getScheme())) {
                    port = 80;
                } else if ("https".equalsIgnoreCase(uri.getScheme())) {
                    port = 443;
                } else {
                    throw new IllegalArgumentException(String.format("The entity [%s] with endpoint [%s] has no port", getClass().getName(), endpoint));
                }
            }
            return HostAndPort.fromParts(uri.getHost(), port);
        } catch (IllegalArgumentException e) {
            // Not a URL; fall-back to host-and-port
        }
        
        HostAndPort result = HostAndPort.fromString(endpoint);
        if (!result.hasPort()) {
            throw new IllegalArgumentException(String.format("The entity [%s] with endpoint [%s] has no port", getClass().getName(), endpoint));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    public void stop() {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPING);
        sensors().set(Attributes.SERVICE_UP, false);
    }

    /**
     * {@inheritDoc}
     */
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }

}