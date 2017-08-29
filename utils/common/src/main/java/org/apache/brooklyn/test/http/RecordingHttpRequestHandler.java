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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.brooklyn.test.Asserts;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class RecordingHttpRequestHandler implements HttpRequestHandler {
    private final HttpRequestHandler delegate;

    private final List<HttpRequest> requests = Lists.newCopyOnWriteArrayList();
    
    public RecordingHttpRequestHandler(HttpRequestHandler delegate) {
        this.delegate = checkNotNull(delegate, "delegate");
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        requests.add(request);
        delegate.handle(request, response, context);
    }

    public void assertHasRequest(Predicate<? super HttpRequest> filter) {
        for (HttpRequest req : requests) {
            if (filter.apply(req)) {
                return;
            }
        }
        Asserts.fail("No request matching filter "+ filter);
    }

    public void assertHasRequestEventually(Predicate<? super HttpRequest> filter) {
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertHasRequest(filter);
            }});
    }
    
    public List<HttpRequest> getRequests(Predicate<? super HttpRequest> filter) {
        return ImmutableList.copyOf(Iterables.filter(requests, filter));
    }

    public List<HttpRequest> getRequests() {
        return ImmutableList.copyOf(requests);
    }
}
