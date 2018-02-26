/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.location.jclouds.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Map;

public class JcloudsContext {

    private String providerOrApi;
    private String identity;
    private String credential;
    private Map<String, String> properties;

    public JcloudsContext() {}

    public JcloudsContext(String providerOrApi) {
        this.providerOrApi = providerOrApi;
        this.identity = null;
        this.credential = null;
        this.properties = Maps.newHashMap();
    }

    public JcloudsContext(String providerOrApi, String identity, String credential, Map<String, String> properties) {
        this.providerOrApi = providerOrApi;
        this.identity = identity;
        this.credential = credential;
        this.properties = properties;
    }

    public String getProviderOrApi() {
        return providerOrApi;
    }

    public void setProviderOrApi(String providerOrApi) {
        this.providerOrApi = providerOrApi;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getCredential() {
        return credential;
    }

    public void setCredential(String credential) {
        this.credential = credential;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JcloudsContext that = (JcloudsContext) o;
        return Objects.equal(providerOrApi, that.providerOrApi) &&
                Objects.equal(identity, that.identity) &&
                Objects.equal(credential, that.credential) &&
                Objects.equal(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(providerOrApi, identity, credential, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("providerOrApi", providerOrApi)
                .add("identity", identity)
                .add("credential", credential)
                .add("properties", properties)
                .toString();
    }
}
