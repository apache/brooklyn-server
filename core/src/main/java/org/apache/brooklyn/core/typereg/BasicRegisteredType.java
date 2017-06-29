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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.osgi.VersionedName;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

/** Instances are usually created by methods in {@link RegisteredTypes}. */
public class BasicRegisteredType implements RegisteredType {

    final RegisteredTypeKind kind;
    final String symbolicName;
    final String version;
    String containingBundle;
    
    final List<OsgiBundleWithUrl> bundles = MutableList.of();
    String displayName;
    String description;
    String iconUrl;
    
    final Set<Object> superTypes = MutableSet.of();
    boolean deprecated;
    boolean disabled;
    final Set<String> aliases = MutableSet.of();
    final Set<Object> tags = MutableSet.of();
    
    TypeImplementationPlan implementationPlan;

    private transient ConfigBag cache = new ConfigBag();
    
    BasicRegisteredType(RegisteredTypeKind kind, String symbolicName, String version, TypeImplementationPlan implementationPlan) {
        this.kind = kind;
        this.symbolicName = symbolicName;
        this.version = version;
        this.implementationPlan = implementationPlan;
    }

    @Override
    public String getId() {
        if (symbolicName==null) return null;
        return symbolicName + (version!=null ? ":"+version : "");
    }
    
    @Override
    public RegisteredTypeKind getKind() {
        return kind;
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
    public VersionedName getVersionedName() {
        return new VersionedName(getSymbolicName(), getVersion());
    }
    
    @Override
    public String getContainingBundle() {
        return containingBundle;
    }
    
    @Override
    public Collection<OsgiBundleWithUrl> getLibraries() {
        return ImmutableSet.copyOf(bundles);
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getIconUrl() {
        return iconUrl;
    }
    
    @Override
    public Set<Object> getSuperTypes() {
        return ImmutableSet.copyOf(superTypes);
    }

    @Override
    public boolean isDisabled() {
        return disabled;
    }
    
    @Override
    public boolean isDeprecated() {
        return deprecated;
    }
    
    @Override
    public Set<String> getAliases() {
        return ImmutableSet.copyOf(aliases);
    }

    @Override
    public Set<Object> getTags() {
        return ImmutableSet.copyOf(tags);
    }

    
    @Beta  // TODO depending how useful this is, it might be better to replace by a static WeakHashMap in RegisteredTypes
    public ConfigBag getCache() {
        return cache;
    }
    
    @Override
    public TypeImplementationPlan getPlan() {
        return implementationPlan;
    }
    
    @Override
    public String toString() {
        return JavaClassNames.simpleClassName(this)+"["+getId()+
            (isDisabled() ? ";DISABLED" : "")+
            (isDeprecated() ? ";deprecated" : "")+
            (getPlan()!=null ? ";"+getPlan().getPlanFormat() : "")+
            "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aliases == null) ? 0 : aliases.hashCode());
        result = prime * result + ((bundles == null) ? 0 : bundles.hashCode());
        result = prime * result + ((containingBundle == null) ? 0 : containingBundle.hashCode());
        result = prime * result + (deprecated ? 1231 : 1237);
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + (disabled ? 1231 : 1237);
        result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
        result = prime * result + ((iconUrl == null) ? 0 : iconUrl.hashCode());
        result = prime * result + ((implementationPlan == null) ? 0 : implementationPlan.hashCode());
        result = prime * result + ((kind == null) ? 0 : kind.hashCode());
        result = prime * result + ((superTypes == null) ? 0 : superTypes.hashCode());
        result = prime * result + ((symbolicName == null) ? 0 : symbolicName.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BasicRegisteredType other = (BasicRegisteredType) obj;
        if (!Objects.equal(aliases, other.aliases)) return false;
        if (!Objects.equal(bundles, other.bundles)) return false;
        if (!Objects.equal(containingBundle, other.containingBundle)) return false;
        if (!Objects.equal(deprecated, other.deprecated)) return false;
        if (!Objects.equal(disabled, other.disabled)) return false;
        if (!Objects.equal(iconUrl, other.iconUrl)) return false;
        if (!Objects.equal(implementationPlan, other.implementationPlan)) return false;
        if (!Objects.equal(kind, other.kind)) return false;
        if (!Objects.equal(superTypes, other.superTypes)) return false;
        if (!Objects.equal(symbolicName, other.symbolicName)) return false;
        if (!Objects.equal(tags, other.tags)) return false;
        if (!Objects.equal(version, other.version)) return false;

        return true;
    }
    
    
}