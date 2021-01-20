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
package org.apache.brooklyn.core.config.external.vault;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.config.external.AbstractExternalConfigSupplier;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class VaultExternalConfigSupplier extends AbstractExternalConfigSupplier {
    public static final String CHARSET_NAME = "UTF-8";
    public static final ImmutableMap<String, String> MINIMAL_HEADERS = ImmutableMap.of(
            "Content-Type", "application/json; charset=" + CHARSET_NAME,
            "Accept", "application/json",
            "Accept-Charset", CHARSET_NAME);
    private static final Logger LOG = LoggerFactory.getLogger(VaultExternalConfigSupplier.class);
    protected final Map<String, String> config;
    protected final String name;
    protected final HttpClient httpClient;
    protected final Gson gson;
    protected final String endpoint;
    protected final String path;
    protected final String mountPoint;
    protected final int version;
    protected final String token;
    protected final ImmutableMap<String, String> headersWithToken;

    public VaultExternalConfigSupplier(ManagementContext managementContext, String name, Map<String, String> config) {
        super(managementContext, name);
        this.config = config;
        this.name = name;
        httpClient = HttpTool.httpClientBuilder().build();
        gson = new GsonBuilder().create();

        List<String> errors = Lists.newArrayListWithCapacity(2);
        endpoint = config.get("endpoint");
        if (Strings.isBlank(endpoint)) errors.add("missing configuration 'endpoint'");
        path = config.get("path");
        if (Strings.isBlank(path)) errors.add("missing configuration 'path'");
        String version = config.get("kv-api-version");
        if (Strings.isBlank(version) || "1".equals(version)) {
            this.version = 1;
        } else if ("2".equals(version)) {
            this.version = 2;
        } else {
            this.version = -1; // satisfy the static analysis :)
            errors.add("'kv-api-version' must be either 1 or 2");
        }
        mountPoint = config.get("mountPoint");
        if (Strings.isBlank(mountPoint) && this.version == 2) errors.add("missing configuration 'mountPoint'");
        if (!Strings.isBlank(mountPoint) && this.version == 1) errors.add("'mountPoint' is only applicable when kv-api-version=2");
        if (!errors.isEmpty()) {
            String message = String.format("Problem configuration Vault external config supplier '%s': %s",
                    name, Joiner.on(System.lineSeparator()).join(errors));
            throw new IllegalArgumentException(message);
        }

        token = initAndLogIn(config);
        headersWithToken = ImmutableMap.<String, String>builder()
                .putAll(MINIMAL_HEADERS)
                .put("X-Vault-Token", token)
                .build();
    }

    protected abstract String initAndLogIn(Map<String, String> config);

    @Override
    public String get(String key) {
        String urlPath = (version == 1)
            ? Urls.mergePaths("v1", path)
            : Urls.mergePaths("v1", mountPoint, "data", path);
        JsonObject response = apiGet(urlPath, headersWithToken);
        JsonElement jsonElement = (version == 1)
            ? response.getAsJsonObject("data").get(key)
            : response.getAsJsonObject("data").getAsJsonObject("data").get(key);
        String asString = jsonElement.getAsString();
        return asString;
    }

    /**
     * Obtains data stored in <code>path</code>.
     */
    public Map<String, String> getDataAsStringMap() {
        JsonObject response = apiGet(Urls.mergePaths("v1", path), headersWithToken);
        Map<String, JsonElement> dataMap = response.getAsJsonObject("data").entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return Maps.transformValues(dataMap, jsonElement -> jsonElement.getAsString());
    }

    protected JsonObject apiGet(String path, ImmutableMap<String, String> headers) {
        try {
            String uri = Urls.mergePaths(endpoint, path);
            LOG.trace("Vault request - GET: {}", uri);
            HttpToolResponse response = HttpTool.httpGet(httpClient, Urls.toUri(uri), headers);
            LOG.trace("Vault response - code: {} {}", response.getResponseCode(), response.getReasonPhrase());
            String responseBody = new String(response.getContent(), CHARSET_NAME);
            if (HttpTool.isStatusCodeHealthy(response.getResponseCode())) {
                return gson.fromJson(responseBody, JsonObject.class);
            } else {
                throw new IllegalStateException("HTTP request returned error");
            }
        } catch (UnsupportedEncodingException e) {
            throw Exceptions.propagate(e);
        }
    }

    protected JsonObject apiPost(String path, ImmutableMap<String, String> headers, ImmutableMap<String, String> requestData) {
        try {
            String body = gson.toJson(requestData);
            String uri = Urls.mergePaths(endpoint, path);
            LOG.trace("Vault request - POST: {}", uri);
            HttpToolResponse response = HttpTool.httpPost(httpClient, Urls.toUri(uri), headers, body.getBytes(CHARSET_NAME));
            LOG.trace("Vault response - code: {} {}", response.getResponseCode(), response.getReasonPhrase());
            String responseBody = new String(response.getContent(), CHARSET_NAME);
            if (HttpTool.isStatusCodeHealthy(response.getResponseCode())) {
                return gson.fromJson(responseBody, JsonObject.class);
            } else {
                throw new IllegalStateException("HTTP request returned error");
            }
        } catch (UnsupportedEncodingException e) {
            throw Exceptions.propagate(e);
        }
    }
}
