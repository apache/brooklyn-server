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
package org.apache.brooklyn.feed.http;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.feed.AttributePollHandler;
import org.apache.brooklyn.core.feed.DelegatingPollHandler;
import org.apache.brooklyn.core.feed.Poller;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.util.executor.HttpExecutorFactory;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.http.executor.UsernamePassword;
import org.apache.brooklyn.util.http.executor.HttpConfig;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.HttpRequest;
import org.apache.brooklyn.util.http.executor.HttpResponse;
import org.apache.brooklyn.util.http.executor.apacheclient.HttpExecutorImpl;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Duration;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;

/**
 * Provides a feed of attribute values, by polling over http.
 * 
 * Example usage (e.g. in an entity that extends SoftwareProcessImpl):
 * <pre>
 * {@code
 * private HttpFeed feed;
 * 
 * //@Override
 * protected void connectSensors() {
 *   super.connectSensors();
 *   
 *   feed = HttpFeed.builder()
 *       .entity(this)
 *       .period(200)
 *       .baseUri(String.format("http://%s:%s/management/subsystem/web/connector/http/read-resource", host, port))
 *       .baseUriVars(ImmutableMap.of("include-runtime","true"))
 *       .poll(new HttpPollConfig<Boolean>(SERVICE_UP)
 *           .onSuccess(HttpValueFunctions.responseCodeEquals(200))
 *           .onError(Functions.constant(false)))
 *       .poll(new HttpPollConfig<Integer>(REQUEST_COUNT)
 *           .onSuccess(HttpValueFunctions.jsonContents("requestCount", Integer.class)))
 *       .build();
 * }
 * 
 * {@literal @}Override
 * protected void disconnectSensors() {
 *   super.disconnectSensors();
 *   if (feed != null) feed.stop();
 * }
 * }
 * </pre>
 * <p>
 *  
 * This also supports giving a Supplier for the URL 
 * (e.g. {@link Entities#attributeSupplier(org.apache.brooklyn.api.entity.Entity, org.apache.brooklyn.api.event.AttributeSensor)})
 * from a sensor.  Note however that if a Supplier-based sensor is *https*,
 * https-specific initialization may not occur if the URL is not available at start time,
 * and it may report errors if that sensor is not available.
 * Some guidance for controlling enablement of a feed based on availability of a sensor
 * can be seen in HttpLatencyDetector (in brooklyn-policy). 
 * 
 * @author aled
 */
public class HttpFeed extends AbstractFeed {

    public static final Logger log = LoggerFactory.getLogger(HttpFeed.class);

    @SuppressWarnings("serial")
    public static final ConfigKey<SetMultimap<HttpPollIdentifier, HttpPollConfig<?>>> POLLS = ConfigKeys.newConfigKey(
            new TypeToken<SetMultimap<HttpPollIdentifier, HttpPollConfig<?>>>() {},
            "polls");

    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Entity entity;
        private boolean onlyIfServiceUp = false;
        private Supplier<URI> baseUriProvider;
        private Duration period = Duration.millis(500);
        private List<HttpPollConfig<?>> polls = Lists.newArrayList();
        private URI baseUri;
        private Map<String, String> baseUriVars = Maps.newLinkedHashMap();
        private Map<String, String> headers = Maps.newLinkedHashMap();
        private boolean suspended = false;
        private Credentials credentials;
        private String uniqueTag;
        private HttpExecutor httpExecutor;
        private volatile boolean built;

        public Builder entity(Entity val) {
            this.entity = val;
            return this;
        }
        public Builder onlyIfServiceUp() { return onlyIfServiceUp(true); }
        public Builder onlyIfServiceUp(boolean onlyIfServiceUp) { 
            this.onlyIfServiceUp = onlyIfServiceUp; 
            return this; 
        }
        public Builder baseUri(Supplier<URI> val) {
            if (baseUri!=null && val!=null)
                throw new IllegalStateException("Builder cannot take both a URI and a URI Provider");
            this.baseUriProvider = val;
            return this;
        }
        public Builder baseUri(URI val) {
            if (baseUriProvider!=null && val!=null)
                throw new IllegalStateException("Builder cannot take both a URI and a URI Provider");
            this.baseUri = val;
            return this;
        }
        public Builder baseUrl(URL val) {
            return baseUri(URI.create(val.toString()));
        }
        public Builder baseUri(String val) {
            return baseUri(URI.create(val));
        }
        public Builder baseUriVars(Map<String,String> vals) {
            baseUriVars.putAll(vals);
            return this;
        }
        public Builder baseUriVar(String key, String val) {
            baseUriVars.put(key, val);
            return this;
        }
        public Builder headers(Map<String,String> vals) {
            headers.putAll(vals);
            return this;
        }
        public Builder header(String key, String val) {
            headers.put(key, val);
            return this;
        }
        public Builder period(Duration duration) {
            this.period = duration;
            return this;
        }
        public Builder period(long millis) {
            return period(millis, TimeUnit.MILLISECONDS);
        }
        public Builder period(long val, TimeUnit units) {
            return period(Duration.of(val, units));
        }
        public Builder poll(HttpPollConfig<?> config) {
            polls.add(config);
            return this;
        }
        public Builder suspended() {
            return suspended(true);
        }
        public Builder suspended(boolean startsSuspended) {
            this.suspended = startsSuspended;
            return this;
        }
        public Builder credentials(String username, String password) {
            this.credentials = new UsernamePasswordCredentials(username, password);
            return this;
        }
        public Builder credentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }
        public Builder credentialsIfNotNull(String username, String password) {
            if (username != null && password != null) {
                this.credentials = new UsernamePasswordCredentials(username, password);
            }
            return this;
        }
        public Builder uniqueTag(String uniqueTag) {
            this.uniqueTag = uniqueTag;
            return this;
        }
        public Builder httpExecutor(HttpExecutor val) {
            this.httpExecutor = val;
            return this;
        }
        public HttpFeed build() {
            built = true;
            HttpFeed result = new HttpFeed(this);
            result.setEntity(checkNotNull((EntityLocal)entity, "entity"));
            if (suspended) result.suspend();
            result.start();
            return result;
        }
        @Override
        protected void finalize() {
            if (!built) log.warn("HttpFeed.Builder created, but build() never called");
        }
    }
    
    private static class HttpPollIdentifier {
        final HttpExecutor httpExecutor;
        final String method;
        final Supplier<URI> uriProvider;
        final Map<String,String> headers;
        final byte[] body;
        final Optional<Credentials> credentials;
        final Duration connectionTimeout;
        final Duration socketTimeout;
        private HttpPollIdentifier(HttpExecutor httpExecutor, String method, Supplier<URI> uriProvider, Map<String, String> headers,
                                   byte[] body, Optional<Credentials> credentials, Duration connectionTimeout, Duration socketTimeout) {
            this.httpExecutor =  httpExecutor;
            this.method = checkNotNull(method, "method").toLowerCase();
            this.uriProvider = checkNotNull(uriProvider, "uriProvider");
            this.headers = checkNotNull(headers, "headers");
            this.body = body;
            this.credentials = checkNotNull(credentials, "credentials");
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            
            if (!(this.method.equals("get") || this.method.equals("post"))) {
                throw new IllegalArgumentException("Unsupported HTTP method (only supports GET and POST): "+method);
            }
            if (body != null && method.equalsIgnoreCase("get")) {
                throw new IllegalArgumentException("Must not set body for http GET method");
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(method, uriProvider, headers, body, credentials);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof HttpPollIdentifier)) {
                return false;
            }
            HttpPollIdentifier o = (HttpPollIdentifier) other;
            return Objects.equal(method, o.method) &&
                    Objects.equal(uriProvider, o.uriProvider) &&
                    Objects.equal(headers, o.headers) &&
                    Objects.equal(body, o.body) &&
                    Objects.equal(httpExecutor, o.httpExecutor) &&
                    Objects.equal(credentials, o.credentials);
        }
    }
    
    /**
     * For rebind; do not call directly; use builder
     */
    public HttpFeed() {
    }
    
    protected HttpFeed(Builder builder) {
        setConfig(ONLY_IF_SERVICE_UP, builder.onlyIfServiceUp);
        Map<String,String> baseHeaders = ImmutableMap.copyOf(checkNotNull(builder.headers, "headers"));

        HttpExecutor httpExecutor;
        if (builder.httpExecutor != null) {
            httpExecutor = builder.httpExecutor;
        } else {
            HttpExecutorFactory httpExecutorFactory = null;
            Collection<? extends Location> locations = Locations.getLocationsCheckingAncestors(builder.entity.getLocations(), builder.entity);
            Maybe<MachineLocation> location =  Machines.findUniqueElement(locations, MachineLocation.class);
            if (location.isPresent() && location.get().hasExtension(HttpExecutorFactory.class)) {
                httpExecutorFactory = location.get().getExtension(HttpExecutorFactory.class);
                Map<String, Object> httpExecutorProps = location.get().getAllConfig(true);
                httpExecutor = httpExecutorFactory.getHttpExecutor(httpExecutorProps);
            } else {
                httpExecutor = HttpExecutorImpl.newInstance();
            }
        }

        SetMultimap<HttpPollIdentifier, HttpPollConfig<?>> polls = HashMultimap.<HttpPollIdentifier,HttpPollConfig<?>>create();
        for (HttpPollConfig<?> config : builder.polls) {
            if (!config.isEnabled()) continue;
            @SuppressWarnings({ "unchecked", "rawtypes" })
            HttpPollConfig<?> configCopy = new HttpPollConfig(config);
            if (configCopy.getPeriod() < 0) configCopy.period(builder.period);
            String method = config.getMethod();
            Map<String,String> headers = config.buildHeaders(baseHeaders);
            byte[] body = config.getBody();
            Duration connectionTimeout = config.getConnectionTimeout();
            Duration socketTimeout = config.getSocketTimeout();
            
            Optional<Credentials> credentials = Optional.fromNullable(builder.credentials);
            
            Supplier<URI> baseUriProvider = builder.baseUriProvider;
            if (builder.baseUri!=null) {
                if (baseUriProvider!=null)
                    throw new IllegalStateException("Not permitted to supply baseUri and baseUriProvider");
                Map<String,String> baseUriVars = ImmutableMap.copyOf(checkNotNull(builder.baseUriVars, "baseUriVars"));
                URI uri = config.buildUri(builder.baseUri, baseUriVars);
                baseUriProvider = Suppliers.ofInstance(uri);
            } else if (!builder.baseUriVars.isEmpty()) {
                throw new IllegalStateException("Not permitted to supply URI vars when using a URI provider; pass the vars to the provider instead");
            }
            checkNotNull(baseUriProvider);

            polls.put(new HttpPollIdentifier(httpExecutor, method, baseUriProvider, headers, body, credentials, connectionTimeout, socketTimeout), configCopy);
        }
        setConfig(POLLS, polls);
        initUniqueTag(builder.uniqueTag, polls.values());
    }

    @Override
    protected void preStart() {
        SetMultimap<HttpPollIdentifier, HttpPollConfig<?>> polls = getConfig(POLLS);

        for (final HttpPollIdentifier pollInfo : polls.keySet()) {
            // Though HttpClients are thread safe and can take advantage of connection pooling
            // and authentication caching, the httpcomponents documentation says:
            //    "While HttpClient instances are thread safe and can be shared between multiple
            //     threads of execution, it is highly recommended that each thread maintains its
            //     own dedicated instance of HttpContext.
            //  http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html

            Set<HttpPollConfig<?>> configs = polls.get(pollInfo);
            long minPeriod = Integer.MAX_VALUE;
            Set<AttributePollHandler<? super HttpToolResponse>> handlers = Sets.newLinkedHashSet();

            for (HttpPollConfig<?> config : configs) {
                handlers.add(new AttributePollHandler<HttpToolResponse>(config, entity, this));
                if (config.getPeriod() > 0) minPeriod = Math.min(minPeriod, config.getPeriod());
            }

            Callable<HttpToolResponse> pollJob;
            pollJob = new Callable<HttpToolResponse>() {
                @Override
                public HttpToolResponse call() throws Exception {
                    if (log.isTraceEnabled()) log.trace("http polling for {} sensors at {}", entity, pollInfo);

                    UsernamePassword creds = null;
                    if (pollInfo.credentials.isPresent()) {
                        creds =  new UsernamePassword(
                                pollInfo.credentials.get().getUserPrincipal().getName(),
                                pollInfo.credentials.get().getPassword());
                    }

                    HttpResponse response =  pollInfo.httpExecutor.execute(new HttpRequest.Builder()
                            .headers(pollInfo.headers)
                            .uri(pollInfo.uriProvider.get())
                            .credentials(creds)
                            .method(pollInfo.method)
                            .body(pollInfo.body)
                            .config(HttpConfig.builder()
                                    .trustSelfSigned(true)
                                    .trustAll(true)
                                    .laxRedirect(true)
                                    .build())
                            .build());
                    return createHttpToolRespose(response);
                }};
                getPoller().scheduleAtFixedRate(pollJob, new DelegatingPollHandler<HttpToolResponse>(handlers), minPeriod);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Poller<HttpToolResponse> getPoller() {
        return  (Poller<HttpToolResponse>) super.getPoller();
    }

    @SuppressWarnings("unchecked")
    private HttpToolResponse createHttpToolRespose(HttpResponse response) throws IOException {
        int responseCode = response.code();

        Map<String,? extends List<String>> headers = (Map<String, List<String>>) (Map<?, ?>) response.headers().asMap();

        byte[] content = null;
        final long durationMillisOfFirstResponse;
        final long durationMillisOfFullContent;
        final long startTime = System.currentTimeMillis();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        durationMillisOfFirstResponse = Duration.sinceUtc(startTime).toMilliseconds();
        try {
            ByteStreams.copy(response.getContent(), out);
            content = out.toByteArray();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            Streams.closeQuietly(out);
        }
        durationMillisOfFullContent = Duration.sinceUtc(startTime).toMilliseconds();

        return new HttpToolResponse(responseCode, headers, content,
                startTime,
                durationMillisOfFirstResponse,
                durationMillisOfFullContent);
    }
}

