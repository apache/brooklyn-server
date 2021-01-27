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
package org.apache.brooklyn.entity.software.base;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.location.access.BrooklynAccessUtils;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.feed.http.HttpFeed;
import org.apache.brooklyn.feed.http.HttpPollConfig;
import org.apache.brooklyn.feed.http.HttpValueFunctions;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.mockwebserver.MockResponse;

/**
 * Test for https://issues.apache.org/jira/browse/BROOKLYN-354.
 * 
 * This recreates the pattern used in NginxController's HttpFeed.
 */
public class SoftwareProcessEntityHttpFeedRebindTest extends RebindTestFixtureWithApp {
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessEntityHttpFeedRebindTest.class);

    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString", "");
    final static ConfigKey<String> MOCK_URL = ConfigKeys.newStringConfigKey("mock.url", "");
    final static ConfigKey<Long> HTTP_POLL_PERIOD = ConfigKeys.newLongConfigKey("httpPollPeriod", "Poll period (in milliseconds)", 50L);

    protected BetterMockWebServer server;
    protected URL baseUrl;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        server = BetterMockWebServer.newInstanceLocalhost();
        for (int i = 0; i < 1000; i++) {
            server.enqueue(new MockResponse().setResponseCode(200).addHeader("content-type: application/json").setBody("\""+i+"\""));
        }
        server.play();
        baseUrl = server.getUrl("/");
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (server != null) server.shutdown();
        }
    }

    @Override
    protected boolean enablePersistenceBackups() {
        return false;
    }

    @Test
    public void testRebind() throws Exception {
        runRebindWithHttpFeed(false);
    }

    /**
     * Test for https://issues.apache.org/jira/browse/BROOKLYN-354.
     * 
     * The entity's {@link HttpFeed} will call {@link BrooklynAccessUtils#getBrooklynAccessibleAddress(org.apache.brooklyn.api.entity.Entity, int)}.
     * In BROOKLYN-354, we saw that this threw an exception if {@link Attributes#HOSTNAME} was null, which caused rebind to fail.
     * 
     * If hostname is null, then we subsequent expect polling to fail because it will not know 
     * what URL to use (in {@link BrooklynAccessUtils#getBrooklynAccessibleAddress(org.apache.brooklyn.api.entity.Entity, int)}).
     * That is something we could look at changing in the future, perhaps.
     */
    @Test
    public void testRebindWithHostnameNull() throws Exception {
        runRebindWithHttpFeed(true);
    }
    
    protected void runRebindWithHttpFeed(boolean setHostnameToNull) throws Exception {
        EmptySoftwareProcess origEntity = origApp.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .impl(SoftwareProcessWithHttpFeedImpl.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .configure(EmptySoftwareProcess.USE_SSH_MONITORING, false)
                .configure(MOCK_URL, baseUrl.toString())
                .location(LocationSpec.create(SshMachineLocation.class)
                        .configure("address", baseUrl.getHost())
                        .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())));
        origApp.start(ImmutableList.<Location>of());
        if (setHostnameToNull) {
            origEntity.sensors().set(EmptySoftwareProcess.HOSTNAME, null);
        }
        
        newApp = rebind();
        EmptySoftwareProcess newEntity = (EmptySoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(EmptySoftwareProcess.class));

        if (setHostnameToNull) {
            EntityAsserts.assertAttributeEqualsEventually(newEntity, SENSOR_STRING, "Failed");
        } else {
            EntityAsserts.assertAttributeChangesEventually(newEntity, SENSOR_STRING);
        }
    }

    public static class SoftwareProcessWithHttpFeedImpl extends EmptySoftwareProcessImpl {
        private HttpFeed httpFeed;
        
        @Override
        public void connectSensors() {
            super.connectSensors();

            ConfigToAttributes.apply(this);

            // "up" is defined as returning a valid HTTP response from nginx (including a 404 etc)
            httpFeed = addFeed(HttpFeed.builder()
                    .uniqueTag("nginx-poll")
                    .entity(this)
                    .period(getConfig(HTTP_POLL_PERIOD))
                    .baseUri(new UrlInferencer())
                    .poll(new HttpPollConfig<String>(SENSOR_STRING)
                            .onResult(HttpValueFunctions.stringContentsFunction())
                            .setOnException("Failed")
                            .suppressDuplicates(true))
                    .build());
        }
        
        @Override
        protected void disconnectSensors() {
            super.disconnectSensors();
            if (httpFeed != null) httpFeed.stop();
        }

        private class UrlInferencer implements Supplier<URI> {
            @Override public URI get() { 
                return URI.create(inferUrl());
            }
        }

        /** returns URL, if it can be inferred; null otherwise */
        protected String inferUrl() {
            URI mockUrl = URI.create(config().get(MOCK_URL));
            Integer port = mockUrl.getPort();
            HostAndPort accessible = BrooklynAccessUtils.getBrooklynAccessibleAddress(this, port);
            if (accessible!=null) {
                try {
                    URI result = new URI(mockUrl.getScheme(), mockUrl.getUserInfo(), accessible.getHostText(), accessible.getPort(), mockUrl.getPath(), mockUrl.getQuery(), mockUrl.getFragment());
                    return result.toString();
                } catch (URISyntaxException e) {
                    throw Exceptions.propagate(e);
                }
            }
            return mockUrl.toString();
        }
    }
}
