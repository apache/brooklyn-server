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
package org.apache.brooklyn.util.http.executor.apacheclient;

import java.io.IOException;
import java.util.Map;

import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.http.executor.HttpConfig;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.HttpRequest;
import org.apache.brooklyn.util.http.executor.HttpResponse;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;

@Beta
public class HttpExecutorImpl implements HttpExecutor {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
    private static final HttpConfig DEFAULT_CONFIG = HttpConfig.builder()
            .laxRedirect(false)
            .trustAll(false)
            .trustSelfSigned(false)
            .build();

    public static HttpExecutorImpl newInstance() {
        return new HttpExecutorImpl();
    }

    /**
     * A must have constructor.
     */
    public HttpExecutorImpl(Map<?, ?> props) {
        
    }
    
    public HttpExecutorImpl() {
    }

    @Override
    public HttpResponse execute(HttpRequest request) throws IOException {
        HttpConfig config = (request.config() != null) ? request.config() : DEFAULT_CONFIG;
        Credentials creds = (request.credentials() != null) ? new UsernamePasswordCredentials(request.credentials().getUser(), request.credentials().getPassword()) : null;
        HttpClient httpClient = HttpTool.httpClientBuilder()
                .uri(request.uri())
                .credential(Optional.fromNullable(creds))
                .laxRedirect(config.laxRedirect())
                .trustSelfSigned(config.trustSelfSigned())
                .trustAll(config.trustAll())
                .build();
        
        HttpToolResponse response;
        
        switch (request.method().toUpperCase()) {
        case HttpExecutor.GET:
            response = HttpTool.httpGet(httpClient, request.uri(), request.headers());
            break;
        case HttpExecutor.HEAD:
            response = HttpTool.httpHead(httpClient, request.uri(), request.headers());
            break;
        case HttpExecutor.POST:
            response = HttpTool.httpPost(httpClient, request.uri(), request.headers(), orEmpty(request.body()));
            break;
        case HttpExecutor.PUT:
            response = HttpTool.httpPut(httpClient, request.uri(), request.headers(), orEmpty(request.body()));
            break;
        case HttpExecutor.DELETE:
            response = HttpTool.httpDelete(httpClient, request.uri(), request.headers());
            break;
        default:
            throw new IllegalArgumentException("Unsupported method '"+request.method()+"' for URI "+request.uri());
        }
        return new HttpResponseWrapper(response);
    }
    
    protected byte[] orEmpty(byte[] val) {
        return (val != null) ? val : EMPTY_BYTE_ARRAY;
    }
}
