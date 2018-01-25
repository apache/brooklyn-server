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

package org.apache.brooklyn.util.core.http;

import org.apache.brooklyn.util.text.Strings;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

public class AuthHandler implements HttpRequestHandler {
    private final String username;
    private final String password;
    private final byte[] responseBody;

    public AuthHandler(String username, String password, String response) {
        this(username, password, response.getBytes());
    }

    public AuthHandler(String username, String password, byte[] response) {
        this.username = username;
        this.password = password;
        this.responseBody = response;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
        String creds = (String) context.getAttribute("creds");
        if (creds == null || !creds.equals(getExpectedCredentials())) {
            response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
        } else {
            response.setEntity(new ByteArrayEntity(responseBody));
        }
    }

    private String getExpectedCredentials() {
        if (Strings.isEmpty(password)) {
            return username;
        } else {
            return username + ":" + password;
        }
    }

}
