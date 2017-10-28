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
package org.apache.brooklyn.core.mgmt.rebind.dto;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.rebind.mementos.ManagedBundleMemento;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.io.ByteSource;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BasicManagedBundleMemento extends AbstractMemento implements ManagedBundleMemento, Serializable {

    private static final long serialVersionUID = -2040630288193425950L;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractMemento.Builder<Builder> {
        protected String symbolicName;
        protected String version;
        protected String url;
        protected String checksum;
        
        public Builder symbolicName(String symbolicName) {
            this.symbolicName = symbolicName;
            return self();
        }
        
        public Builder version(String version) {
            this.version = version;
            return self();
        }

        public Builder url(String url) {
            this.url = url;
            return self();
        }

        public Builder checksum(String checksum) {
            this.checksum = checksum;
            return self();
        }

        public Builder from(ManagedBundleMemento other) {
            super.from(other);
            symbolicName = other.getSymbolicName();
            version = other.getVersion();
            url = other.getUrl();
            checksum = other.getChecksum();
            return self();
        }

        public BasicManagedBundleMemento build() {
            return new BasicManagedBundleMemento(this);
        }
    }

    private String symbolicName;
    private String version;
    private String url;
    private String checksum;
    transient private ByteSource jarContent;

    @SuppressWarnings("unused") // For deserialisation
    private BasicManagedBundleMemento() {}

    protected BasicManagedBundleMemento(Builder builder) {
        super(builder);
        this.symbolicName = builder.symbolicName;
        this.version = builder.version;
        this.url = builder.url;
        this.checksum = builder.checksum;
    }

    @Override
    public String getSymbolicName() {
        return symbolicName;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String getChecksum() {
        return checksum;
    }

    @Override
    public ByteSource getJarContent() {
        return jarContent;
    }
    
    @Override
    public void setJarContent(ByteSource byteSource) {
        this.jarContent = byteSource;
    }

    @Override
    protected void setCustomFields(Map<String, Object> fields) {
        if (!fields.isEmpty()) {
            throw new UnsupportedOperationException("Cannot set custom fields on " + this + ". " +
                    "Fields=" + Joiner.on(", ").join(fields.keySet()));
        }
    }

    @Override
    public Map<String, ? extends Object> getCustomFields() {
        return Collections.emptyMap();
    }

    @Override
    protected MoreObjects.ToStringHelper newVerboseStringHelper() {
        return super.newVerboseStringHelper()
                .add("symbolicName", getSymbolicName())
                .add("version", getVersion())
                .add("url", getUrl())
                .add("checksum", getChecksum());
    }
}
