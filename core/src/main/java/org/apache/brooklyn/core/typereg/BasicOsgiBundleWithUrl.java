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
package org.apache.brooklyn.core.typereg;

import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BasicOsgiBundleWithUrl implements CatalogBundle, OsgiBundleWithUrl {
    private String symbolicName;
    private String version;
    private String url;

    // for deserializing (not sure if needed?)
    @SuppressWarnings("unused")
    private BasicOsgiBundleWithUrl() {}

    public BasicOsgiBundleWithUrl(String name, String version, String url) {
        if (name == null && version == null) {
            Preconditions.checkNotNull(url, "Either a URL or both name and version are required");
        } else {
            Preconditions.checkNotNull(name, "Either a URL or both name and version are required");
            Preconditions.checkNotNull(version, "Either a URL or both name and version are required");
        }

        this.symbolicName = name;
        this.version = version;
        this.url = url;
    }
    
    public BasicOsgiBundleWithUrl(OsgiBundleWithUrl b) {
        this(b.getSymbolicName(), b.getSuppliedVersionString(), b.getUrl());
    }

    @Override
    public boolean isNameResolved() {
        return symbolicName != null && version != null;
    }
    
    @Override
    @Deprecated //see super
    public boolean isNamed() {
        return isNameResolved();
    }

    @Override
    public String getSymbolicName() {
        return symbolicName;
    }

    @Override
    public String getSuppliedVersionString() {
        return version;
    }
    
    @Override
    public String getOsgiVersionString() {
        return version==null ? version : BrooklynVersionSyntax.toValidOsgiVersion(version);
    }

    @Override
    public VersionedName getVersionedName() {
        if (!isNameResolved()) return null;
        return new VersionedName(getSymbolicName(), getSuppliedVersionString());
    }
    
    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("symbolicName", symbolicName)
                .add("version", version)
                .add("url", url)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(symbolicName, version, url);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        OsgiBundleWithUrl other = (OsgiBundleWithUrl) obj;
        if (!Objects.equal(symbolicName, other.getSymbolicName())) return false;
        if (!Objects.equal(getOsgiVersionString(), other.getOsgiVersionString())) return false;
        if (!Objects.equal(url, other.getUrl())) return false;
        return true;
    }
    
}
