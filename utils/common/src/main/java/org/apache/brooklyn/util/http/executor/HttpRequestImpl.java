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
package org.apache.brooklyn.util.http.executor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;

import org.apache.brooklyn.util.http.executor.Credentials;

import com.google.common.annotations.Beta;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * A (mostly) immutable http request. However, (for efficiency reasons) we do not copy the body.
 */
@Beta
public class HttpRequestImpl implements HttpRequest {

    protected final URI uri;
    protected final String method;
    protected final byte[] body;
    protected final Multimap<String, String> headers;
    protected final Credentials credentials;
    protected final HttpConfig config;

    protected HttpRequestImpl(HttpRequest.Builder builder) {
        this.uri = checkNotNull(builder.uri, "uri");
        this.method = checkNotNull(builder.method, "method");
        this.body = builder.body;
        this.headers = Multimaps.unmodifiableMultimap(ArrayListMultimap.create(checkNotNull(builder.headers, "headers")));
        this.credentials = builder.credentials;
        this.config = builder.config;
    }

    public HttpRequestImpl(HttpRequestImpl httpRequest) {
        this.uri = checkNotNull(httpRequest.uri, "uri");
        this.method = checkNotNull(httpRequest.method, "method");
        this.body = httpRequest.body;
        this.headers = Multimaps.unmodifiableMultimap(ArrayListMultimap.create(checkNotNull(httpRequest.headers, "headers")));
        this.credentials = httpRequest.credentials;
        this.config = httpRequest.config;
    }

    @Override
    public URI uri() {
        return uri;
    }
    
    @Override
    public String method() {
        return method;
    }
    
    @Override
    public byte[] body() {
        return body;
    }
    
    @Override
    public Multimap<String, String> headers() {
        return headers;
    }
    
    @Override
    public Credentials credentials() {
        return credentials;
    }
    
    @Override
    public HttpConfig config() {
        return config;
    }
}
