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
package org.apache.brooklyn.core.catalog.internal;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.http.auth.Credentials;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.osgi.framework.Bundle;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class CatalogBundleDto implements CatalogBundle {
    private String symbolicName;
    private String version;
    private String url;
    private Credentials credential;

    public CatalogBundleDto() {}

    public CatalogBundleDto(String name, String version, String url) {
        this(name, version, url, null);
    }

    public CatalogBundleDto(String name, String version, String url, @Nullable Credentials credential) {
        if (name == null && version == null) {
            Preconditions.checkNotNull(url, "url to an OSGi bundle is required");
        } else {
            Preconditions.checkNotNull(name, "both name and version are required");
            Preconditions.checkNotNull(version, "both name and version are required");
        }

        this.symbolicName = name;
        this.version = version==null ? null : BrooklynVersionSyntax.toValidOsgiVersion(version);
        this.url = url;
        this.credential = credential;
    }

    @Override
    public boolean isNameResolved() {
        return symbolicName != null && version != null;
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
    public Credentials getUrlCredential() {
        return credential;
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
        CatalogBundleDto other = (CatalogBundleDto) obj;
        if (!Objects.equal(symbolicName, other.symbolicName)) return false;
        if (!Objects.equal(version, other.version)) return false;
        if (!Objects.equal(url, other.url)) return false;
        return true;
    }
    
    public static Maybe<CatalogBundle> resolve(ManagementContext mgmt, CatalogBundle b) {
        if (b.isNameResolved()) return Maybe.of(b);
        OsgiManager osgi = ((ManagementContextInternal)mgmt).getOsgiManager().orNull();
        if (osgi==null) return Maybe.absent("No OSGi manager");
        Maybe<Bundle> b2 = osgi.findBundle(b);
        if (b2.isAbsent()) return Maybe.absent("Nothing installed for "+b);
        return Maybe.of(new CatalogBundleDto(b2.get().getSymbolicName(), b2.get().getVersion().toString(), b.getUrl()));
    }
    
}
