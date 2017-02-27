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

package org.apache.brooklyn.location.jclouds;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.brooklyn.core.config.Sanitizer;
import org.jclouds.domain.LoginCredentials;

import com.google.common.base.MoreObjects;
import com.google.common.net.HostAndPort;

public class ManagementAddressResolveResult {
    private final HostAndPort hostAndPort;
    private final LoginCredentials credentials;

    ManagementAddressResolveResult(HostAndPort hostAndPort, LoginCredentials credentials) {
        this.hostAndPort = checkNotNull(hostAndPort, "hostAndPort");
        this.credentials = checkNotNull(credentials, "credentials");
    }

    public HostAndPort hostAndPort() {
        return hostAndPort;
    }

    public LoginCredentials credentials() {
        return credentials;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("hostAndPort", hostAndPort)
                .add("credentials", credentials)
                .toString();
    }
}
