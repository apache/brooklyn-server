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
package org.apache.brooklyn.core.sensor.http;

import java.net.URI;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.feed.http.HttpFeed;
import org.apache.brooklyn.feed.http.HttpPollConfig;
import org.apache.brooklyn.feed.http.HttpValueFunctions;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;

import net.minidev.json.JSONObject;

/**
 * Configurable {@link org.apache.brooklyn.api.entity.EntityInitializer} which adds an HTTP sensor feed to retrieve the
 * {@link JSONObject} from a JSON response in order to populate the sensor with the data at the {@code jsonPath}.
 *
 * @see SshCommandSensor
 */
@Beta
public final class HttpRequestSensor<T> extends AddSensor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestSensor.class);

    public static final ConfigKey<String> SENSOR_URI = ConfigKeys.newStringConfigKey("uri", "HTTP URI to poll for JSON");
    public static final ConfigKey<String> JSON_PATH = ConfigKeys.newStringConfigKey("jsonPath", "JSON path to select in HTTP response; default $", "$");
    public static final ConfigKey<String> USERNAME = ConfigKeys.newStringConfigKey("username", "Username for HTTP request, if required");
    public static final ConfigKey<String> PASSWORD = ConfigKeys.newStringConfigKey("password", "Password for HTTP request, if required");
    public static final ConfigKey<Map<String, String>> HEADERS = new MapConfigKey(String.class, "headers");

    public HttpRequestSensor(final ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        super.apply(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding HTTP JSON sensor {} to {}", name, entity);
        }

        final ConfigBag allConfig = ConfigBag.newInstanceCopying(this.params).putAll(params);
        final Supplier<URI> uri = new Supplier<URI>() {
            @Override
            public URI get() {
                return URI.create(EntityInitializers.resolve(allConfig, SENSOR_URI));
            }
        };
        final String jsonPath = EntityInitializers.resolve(allConfig, JSON_PATH);
        final String username = EntityInitializers.resolve(allConfig, USERNAME);
        final String password = EntityInitializers.resolve(allConfig, PASSWORD);
        final Map<String, String> headers = EntityInitializers.resolve(allConfig, HEADERS);

        
        HttpPollConfig<T> pollConfig = new HttpPollConfig<T>(sensor)
                .checkSuccess(HttpValueFunctions.responseCodeEquals(200))
                .onFailureOrException(Functions.constant((T) null))
                .onSuccess(HttpValueFunctions.<T>jsonContentsFromPath(jsonPath))
                .period(period);

        HttpFeed.Builder httpRequestBuilder = HttpFeed.builder().entity(entity)
                .baseUri(uri)
                .credentialsIfNotNull(username, password)
                .poll(pollConfig);

        if (headers != null) {
            httpRequestBuilder.headers(headers);
        }
        
        HttpFeed feed = httpRequestBuilder.build();
        entity.addFeed(feed);
    }

}
