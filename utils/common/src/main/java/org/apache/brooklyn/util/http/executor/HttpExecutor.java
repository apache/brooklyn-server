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

import java.io.IOException;

/**
 * An abstraction for executing HTTP requests, allowing an appropriate implementation to be chosen.
 */
public interface HttpExecutor {
    /**
     * HTTP methods
     */
    static final String GET = "GET";

    static final String HEAD = "HEAD";

    static final String POST = "POST";

    static final String PUT = "PUT";

    static final String DELETE = "DELETE";

    /**
     * Synchronously send the request, and return its response.
     * 
     * To avoid leaking resources callers must close the response's content (or the response itself).
     *  
     * @throws IOException if a problem occurred talking to the server.
     * @throws RuntimeException (and subclasses) if an unexpected error occurs creating the request.
     */
    HttpResponse execute(HttpRequest request) throws IOException;
}
