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
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.http.auth.Credentials;

import com.google.common.annotations.Beta;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

@Beta
public interface HttpRequest {
    
    // TODO Should we use InputStream for body (rather than byte[]). That would mean we don't have
    // to hold the entire body in-memory, which might be important for really big payloads!

    @Beta
    public static class Builder {
        protected URI uri;
        protected String method;
        protected byte[] body;
        protected Multimap<String, String> headers = ArrayListMultimap.<String, String>create();
        protected Credentials credentials;
        protected HttpConfig config;

        public Builder uri(URI val) {
            uri = checkNotNull(val, "uri");
            return this;
        }

        public Builder method(String val) {
            method = checkNotNull(val, "method");
            return this;
        }

        /**
         * This val must not be modified after being passed in - the object will be used while executing the request.
         */
        public Builder body(@Nullable byte[] val) {
            body = val;
            return this;
        }

        public Builder headers(Multimap<String, String> val) {
            headers.putAll(checkNotNull(val, "headers"));
            return this;
        }

        public Builder headers(Map<String, String> val) {
            if (checkNotNull(val, "headers").keySet().contains(null)) {
                throw new NullPointerException("Headers must not contain null key");
            }
            for (Map.Entry<String, String> entry : val.entrySet()) {
                header(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder header(String key, String val) {
            headers.put(checkNotNull(key, "key"), val);
            return this;
        }

        public Builder credentials(@Nullable Credentials val) {
            credentials = val;
            return this;
        }

        public Builder config(@Nullable HttpConfig val) {
            config = val;
            return this;
        }

        public Builder from(HttpRequestImpl httpRequest) {
            this.uri = checkNotNull(httpRequest.uri, "uri");
            this.method = checkNotNull(httpRequest.method, "method");
            this.body = httpRequest.body;
            this.headers = ArrayListMultimap.create(checkNotNull(httpRequest.headers, "headers"));
            this.credentials = httpRequest.credentials;
            this.config = httpRequest.config;
            return this;
        }

        public HttpRequest build() {
            return new HttpRequestImpl(this);
        }
    }

    URI uri();

    String method();

    /**
     * The payload of the request, or null if no payload (e.g. for GET requests). 
     */
    @Nullable
    byte[] body();
    
    Multimap<String, String> headers();

    @Nullable
    Credentials credentials();

    /**
     * Additional optional configuration to customize how the call is done.
     */
    @Nullable
    HttpConfig config();
}
