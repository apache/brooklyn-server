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
package org.apache.brooklyn.util.core.logbook.opensearch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.minidev.json.JSONObject;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.brooklyn.util.core.logbook.LogBookQueryParams;
import org.apache.brooklyn.util.core.logbook.LogStore;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.brooklyn.util.core.logbook.LogbookConfig.BASE_NAME_LOGBOOK;

/**
 * Implementation for expose log from ElasticSearch to the logbook API.
 */
public class OpenSearchLogStore implements LogStore {

    /*
     # example config for local default implementation
     brooklyn.logbook.logStore = org.apache.brooklyn.util.core.logbook.opensearch.OpenSearchLogStore
     brooklyn.logbook.openSearchLogStore.host = https://localhost:9200
     brooklyn.logbook.openSearchLogStore.index = brooklyn8
     brooklyn.logbook.openSearchLogStore.user = admin
     brooklyn.logbook.openSearchLogStore.password = admin
     brooklyn.logbook.openSearchLogStore.verifySsl = false
     */
    public final static String BASE_NAME_OPEN_SEARCH_LOG_STORE = BASE_NAME_LOGBOOK + ".openSearchLogStore";

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_HOST = ConfigKeys.newStringConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".host", "Log store host");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_INDEX = ConfigKeys.newStringConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".index", "Log store index");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_USER = ConfigKeys.newStringConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".user", "User name");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_PASS = ConfigKeys.newStringConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".password", "User password");

    public final static ConfigKey<String> LOGBOOK_LOG_STORE_APIKEY = ConfigKeys.newStringConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".apikey", "API key");

    public final static ConfigKey<Boolean> LOGBOOK_LOG_STORE_VERIFY_SSL = ConfigKeys.newBooleanConfigKey(
            BASE_NAME_OPEN_SEARCH_LOG_STORE + ".verifySsl", "Verify SSL", true);

    private final ManagementContext mgmt;
    CloseableHttpClient httpClient;
    private String host;
    private String user;
    private String password;
    private String apiKey;
    private Boolean verifySsl;
    private String indexName;

    @VisibleForTesting
    public OpenSearchLogStore() {
        this.mgmt = null;
    }

    public OpenSearchLogStore(ManagementContext mgmt) {
        this.mgmt = mgmt;
        initialize();

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        if (!verifySsl) {

            final SSLContext sslContext;
            try {
                sslContext = SSLContextBuilder
                        .create()
                        .loadTrustMaterial(new TrustSelfSignedStrategy())
                        .build();
                HostnameVerifier allowAllHosts = new NoopHostnameVerifier();
                SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext, allowAllHosts);
                httpClientBuilder.setSSLSocketFactory(connectionFactory);
            } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
                Exceptions.propagate(e);
            }
        }
        if (Strings.isNonBlank(apiKey)) {
            httpClientBuilder.setDefaultHeaders(ImmutableList.of(new BasicHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + apiKey)));
        } else {
            httpClientBuilder.setDefaultCredentialsProvider(buildBasicCredentialsProvider());
        }

        httpClient = httpClientBuilder.build();
    }

    private CredentialsProvider buildBasicCredentialsProvider() {
        URL url = null;
        try {
            url = new URL(host);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("The provided host OpenSearch host URL is not valid: " + host);
        }
        HttpHost httpHost = new HttpHost(url.getHost(), url.getPort());
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(
                new AuthScope(httpHost),
                new UsernamePasswordCredentials(user, password)
        );
        return provider;
    }

    private void initialize() {
        this.host = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_HOST);
        Preconditions.checkNotNull(host, "OpenSearch host must be set: " + LOGBOOK_LOG_STORE_HOST.getName());

        this.user = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_USER);
        this.indexName = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_INDEX);
        this.password = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_PASS); // TODO: this is not completely secure
        this.apiKey = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_APIKEY);
        this.verifySsl = mgmt.getConfig().getConfig(LOGBOOK_LOG_STORE_VERIFY_SSL);
    }

    @Override
    public List<BrooklynLogEntry> query(LogBookQueryParams params) throws IOException {
        HttpPost request = new HttpPost(host + "/" + indexName + "/_search");
        request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        request.setEntity(new StringEntity(getJSONQuery(params)));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            BrooklynOpenSearchModel jsonResponse = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(response.getEntity().getContent(), BrooklynOpenSearchModel.class);
            if (jsonResponse.hits != null && jsonResponse.hits.hits != null) {
                return jsonResponse.hits.hits.stream()
                        .map(BrooklynOpenSearchModel.OpenSearchHit::getSource)
                        .collect(Collectors.toList());
            } else {
                return ImmutableList.of();
            }
        }
    }

    @VisibleForTesting
    protected String getJSONQuery(LogBookQueryParams params) {
        ImmutableMap qb = ImmutableMap.builder()
                .put("size", params.getNumberOfItems())
                .put("sort", ImmutableMap.of("timestamp", params.getReverseOrder() ? "desc" : "asc"))
                .put("query", buildQuery(params))
                .build();
        return new JSONObject(qb).toString();
    }

    private ImmutableMap<String, Object> buildQuery(LogBookQueryParams params) {
        boolean noConditions = true;
        ImmutableList.Builder<Object> conditionsBuilder = ImmutableList.builder();

        if (!params.getLevels().isEmpty() && !params.getLevels().contains("ALL")) {
            conditionsBuilder.add(ImmutableMap.of("terms",
                    ImmutableMap.of("level", ImmutableList.copyOf(params.getLevels().stream().map(String::toLowerCase).map(String::trim).collect(Collectors.toList())))));
            noConditions = false;
        }
        if (Strings.isNonBlank(params.getInitTime()) || Strings.isNonBlank(params.getFinalTime())) {
            ImmutableMap.Builder<Object, Object> timestampMapBuilder = ImmutableMap.builder();
            if (Strings.isNonBlank(params.getInitTime())) {
                timestampMapBuilder.put("gte", params.getInitTime());
            }
            if (Strings.isNonBlank(params.getFinalTime())) {
                timestampMapBuilder.put("lte", params.getFinalTime());
            }
            conditionsBuilder.add(ImmutableMap.of("range", ImmutableMap.of("timestamp", timestampMapBuilder.build())));
            noConditions = false;
        }

        if (noConditions) {
            return ImmutableMap.of("match_all", ImmutableMap.of());
        } else {
            return ImmutableMap.of("bool", ImmutableMap.of("must", conditionsBuilder.build()));
        }

    }

}
