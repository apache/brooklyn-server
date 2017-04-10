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

import java.util.Map;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.time.Duration;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;

import com.google.common.reflect.TypeToken;

/**
 * Entity that makes a HTTP Request and tests the response
 */
@ImplementedBy(value = TestHttpCallImpl.class)
public interface TestHttpCall extends BaseTest {

    @SetFromFlag(nullable = false)
    ConfigKey<String> TARGET_URL = ConfigKeys.newStringConfigKey("url", "URL to test");

    @SetFromFlag(nullable = false)
    ConfigKey<HttpMethod> TARGET_METHOD = ConfigKeys.builder(HttpMethod.class)
            .name("method")
            .description("Method to request the URL: GET, POST, PUT, DELETE, etc")
            .defaultValue(HttpMethod.GET)
            .build();

    ConfigKey<Boolean> TRUST_ALL = ConfigKeys.newBooleanConfigKey("trustAll","Trust all certificates used to sign this request",true);

    ConfigKey<Map<String, String>> TARGET_HEADERS = ConfigKeys.builder(new TypeToken<Map<String, String>>() {})
            .name("headers")
            .description("Headers to add to the request")
            .build();

    ConfigKey<String> TARGET_BODY = ConfigKeys.newStringConfigKey("body", "The request body to send (only for POST and PUT requests)");

    ConfigKey<HttpAssertionTarget> ASSERTION_TARGET = ConfigKeys.newConfigKey(HttpAssertionTarget.class, "applyAssertionTo",
        "The HTTP field to apply the assertion to [body,status]", HttpAssertionTarget.body);

    /**
     * The maximum number of times to execute the http call, before throwing an exception.
     */
    ConfigKey<Integer> MAX_ATTEMPTS = ConfigKeys.newIntegerConfigKey("maxAttempts", "Maximum number of attempts");

    enum HttpMethod {
        GET(HttpGet.class),
        POST(HttpPost.class),
        PUT(HttpPut.class),
        DELETE(HttpDelete.class),
        HEAD(HttpHead.class),
        OPTIONS(HttpOptions.class),
        TRACE(HttpTrace.class);

        public final Class<? extends HttpRequestBase> requestClass;

        HttpMethod(Class<? extends HttpRequestBase> requestClass) {
            this.requestClass = requestClass;
        }

        @Override
        public String toString() {
            return this.name();
        }
    }

    enum HttpAssertionTarget {
        body("body"), status("status");
        private final String httpAssertionTarget;

        HttpAssertionTarget(final String httpAssertionTarget) {
            this.httpAssertionTarget = httpAssertionTarget;
        }

        @Override
        public String toString() {
            return httpAssertionTarget;
        }
    }

}
