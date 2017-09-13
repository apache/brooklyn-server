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
package org.apache.brooklyn.rest.domain;

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.VersionComparator;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.ComparisonChain;

public class TypeSummary implements Comparable<TypeSummary> {

    private final String symbolicName;
    private final String version;
    private final String containingBundle;
    private final RegisteredTypeKind kind;
    
    @JsonInclude(value=Include.NON_EMPTY)
    private final String displayName;
    @JsonInclude(value=Include.NON_EMPTY)
    private final String description;
    @JsonInclude(value=Include.NON_EMPTY)
    private String iconUrl;

    @JsonInclude(value=Include.NON_EMPTY)
    private Set<String> aliases;
    @JsonInclude(value=Include.NON_EMPTY)
    private Set<Object> supertypes;
    @JsonInclude(value=Include.NON_EMPTY)
    private Set<Object> tags;

    @JsonInclude(value=Include.NON_DEFAULT)
    private boolean disabled = false;
    @JsonInclude(value=Include.NON_DEFAULT)
    private boolean deprecated = false;
    
    // not exported directly, but used to provide other top-level json fields
    // for specific types
    @JsonIgnore
    private final Map<String,Object> others = MutableMap.of();
    
    /** Constructor for JSON deserialization use only. */
    TypeSummary() {
        symbolicName = null;
        version = null;
        containingBundle = null;
        kind = null;
        
        displayName = null;
        description = null;
    }
    
    public TypeSummary(RegisteredType t) {
        symbolicName = t.getSymbolicName();
        version = t.getVersion();
        containingBundle = t.getContainingBundle();
        kind = t.getKind();
        
        displayName = t.getDisplayName();
        description = t.getDescription();
        iconUrl = t.getIconUrl();
        
        aliases = t.getAliases();
        supertypes = t.getSuperTypes();
        tags = t.getTags();

        deprecated = t.isDeprecated();
        disabled = t.isDisabled();
    }
    
    public TypeSummary(TypeSummary t) {
        symbolicName = t.getSymbolicName();
        version = t.getVersion();
        containingBundle = t.getContainingBundle();
        kind = t.getKind();
        
        displayName = t.getDisplayName();
        description = t.getDescription();
        iconUrl = t.getIconUrl();
        
        aliases = t.getAliases();
        supertypes = t.getSupertypes();
        tags = t.getTags();

        deprecated = t.isDeprecated();
        disabled = t.isDisabled();
        
        others.putAll(t.getExtraFields());
    }

    /** Mutable map of other top-level metadata included on this DTO (eg listing config keys or effectors) */ 
    @JsonAnyGetter 
    public Map<String,Object> getExtraFields() {
        return others;
    }
    @JsonAnySetter
    public void setExtraField(String name, Object value) {
        others.put(name, value);
    }
    
    public void setIconUrl(String iconUrl) {
        this.iconUrl = iconUrl;
    }
    
    @Override
    public int compareTo(TypeSummary o2) {
        TypeSummary o1 = this;
        return ComparisonChain.start()
            .compare(o1.symbolicName, o2.symbolicName, NaturalOrderComparator.INSTANCE)
            .compareFalseFirst(o1.disabled, o2.disabled)
            .compareFalseFirst(o1.deprecated, o2.deprecated)
            .compare(o2.version, o1.version, VersionComparator.INSTANCE)
            .result();
    }

    public String getSymbolicName() {
        return symbolicName;
    }

    public String getVersion() {
        return version;
    }

    public String getContainingBundle() {
        return containingBundle;
    }

    public RegisteredTypeKind getKind() {
        return kind;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public String getIconUrl() {
        return iconUrl;
    }

    public Set<String> getAliases() {
        return aliases;
    }

    public Set<Object> getSupertypes() {
        return supertypes;
    }

    public Set<Object> getTags() {
        return tags;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aliases == null) ? 0 : aliases.hashCode());
        result = prime * result + ((containingBundle == null) ? 0 : containingBundle.hashCode());
        result = prime * result + (deprecated ? 1231 : 1237);
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + (disabled ? 1231 : 1237);
        result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
        result = prime * result + ((iconUrl == null) ? 0 : iconUrl.hashCode());
        result = prime * result + ((kind == null) ? 0 : kind.hashCode());
        // don't use 'others' - see equals comment
        // result = prime * result + ((others == null) ? 0 : others.hashCode());
        result = prime * result + ((supertypes == null) ? 0 : supertypes.hashCode());
        result = prime * result + ((symbolicName == null) ? 0 : symbolicName.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeSummary other = (TypeSummary) obj;
        if (aliases == null) {
            if (other.aliases != null)
                return false;
        } else if (!aliases.equals(other.aliases))
            return false;
        if (containingBundle == null) {
            if (other.containingBundle != null)
                return false;
        } else if (!containingBundle.equals(other.containingBundle))
            return false;
        if (deprecated != other.deprecated)
            return false;
        if (description == null) {
            if (other.description != null)
                return false;
        } else if (!description.equals(other.description))
            return false;
        if (disabled != other.disabled)
            return false;
        if (displayName == null) {
            if (other.displayName != null)
                return false;
        } else if (!displayName.equals(other.displayName))
            return false;
        if (iconUrl == null) {
            if (other.iconUrl != null)
                return false;
        } else if (!iconUrl.equals(other.iconUrl))
            return false;
        if (kind != other.kind)
            return false;
        // don't compare "others" -- eg if "links" are set, we don't care
//        if (others == null) {
//            if (other.others != null)
//                return false;
//        } else if (!others.equals(other.others))
//            return false;
        if (supertypes == null) {
            if (other.supertypes != null)
                return false;
        } else if (!supertypes.equals(other.supertypes))
            return false;
        if (symbolicName == null) {
            if (other.symbolicName != null)
                return false;
        } else if (!symbolicName.equals(other.symbolicName))
            return false;
        if (tags == null) {
            if (other.tags != null)
                return false;
        } else if (!tags.equals(other.tags))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return JavaClassNames.cleanSimpleClassName(this)+"["+symbolicName+":"+version+
            ", containingBundle=" + containingBundle + ", kind=" + kind + ", displayName=" + displayName + "]";
    }
    
}
