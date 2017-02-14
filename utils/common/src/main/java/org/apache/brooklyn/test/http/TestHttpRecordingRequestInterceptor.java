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
package org.apache.brooklyn.test.http;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestHttpRecordingRequestInterceptor implements HttpRequestInterceptor {

    private final List<HttpRequest> requests = Collections.synchronizedList(Lists.<HttpRequest>newArrayList());
    
    public List<HttpRequest> getRequests() {
        synchronized (requests) {
            return ImmutableList.copyOf(requests);
        }
    }
    
    public HttpRequest getLastRequest() {
        synchronized (requests) {
            if (requests.isEmpty()) throw new NoSuchElementException("No http-requests received");
            return requests.get(requests.size()-1);
        }
    }
    
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        requests.add(request);
    }
}
