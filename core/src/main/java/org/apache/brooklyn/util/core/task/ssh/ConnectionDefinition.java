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
package org.apache.brooklyn.util.core.task.ssh;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Secret;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ConnectionDefinition {

    public static final String CONNECTION = "connection";

    public static final List<String> CONNECTION_TYPES = ImmutableList.of("ssh", "winrm");

    String type;
    WrappedValue<String> user;
    WrappedValue<Secret<String>> password;

    // TODO next 3 should also be wrapped values
    Secret<String> private_key;
    String host;
    String port;

    public ConnectionDefinition() {
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    protected Map<String,Object> other = MutableMap.of();

    @JsonAnySetter
    public void setOther(String k, Object v) {
        other.put(k,v);
    }

    public String getType() {
        return type;
    }

    public WrappedValue<String> getUser() {
        return user;
    }

    public WrappedValue<Secret<String>> getPassword() {
        return password;
    }

    public Secret<String> getPrivate_key() {
        return private_key;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUser(WrappedValue<String> user) {
        this.user = user;
    }

    public void setPassword(WrappedValue<Secret<String>> password) {
        this.password = password;
    }

    public void setPrivate_key(String private_key) {
        this.private_key = new Secret<>(private_key);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectionDefinition that = (ConnectionDefinition) o;
        return Objects.equals(type, that.type) && Objects.equals(user, that.user)
                && Objects.equals(password, that.password)
                && Objects.equals(host, that.host)
                && Objects.equals(port, that.port)
                && Objects.equals(other, that.other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, user, password, host, port, other);
    }

}
