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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;
import org.osgi.framework.VersionRange;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public class BundleUpgradeParser {

    @Beta
    public static final String MANIFEST_HEADER_REMOVE_LEGACY_ITEMS = "brooklyn-catalog-force-remove-legacy-items";

    public static class TypeUpgrades {
        static final TypeUpgrades EMPTY = new TypeUpgrades(ImmutableSet.of());
        
        static class Builder {
            private Set<VersionRangedName> removedLegacyItems = new LinkedHashSet<>();
            
            public void addAll(TypeUpgrades other) {
                removedLegacyItems.addAll(other.removedLegacyItems);
            }
            public TypeUpgrades build() {
                return new TypeUpgrades(removedLegacyItems);
            }
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        private Set<VersionRangedName> removedLegacyItems; // TODO Want version ranges as well
        
        public TypeUpgrades(Iterable<? extends VersionRangedName> removedLegacyItems) {
            this.removedLegacyItems = ImmutableSet.copyOf(removedLegacyItems);
        }

        public boolean isEmpty() {
            return removedLegacyItems.isEmpty();
        }

        public boolean isRemoved(CatalogItem<?, ?> legacyCatalogItem) {
            String name = legacyCatalogItem.getSymbolicName();
            String versionStr = legacyCatalogItem.getVersion();
            Version version = Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion(versionStr == null ? BrooklynCatalog.DEFAULT_VERSION : versionStr));
            
            for (VersionRangedName removedLegacyItem : removedLegacyItems) {
                if (removedLegacyItem.getSymbolicName().equals(name) && removedLegacyItem.getOsgiVersionRange().includes(version)) {
                    return true;
                }
            }
            return false;
        }
    }
    
    /** Records a name (string) and version range (string),
     * with conveniences for pretty-printing and converting to OSGi format. */
    public static class VersionRangedName {
        private final String name;
        private final String v;
        
        public static VersionRangedName fromString(String val, boolean singleVersionIsOsgiRange) {
            if (Strings.isBlank(val)) {
                throw new IllegalArgumentException("Must not be blank");
            }
            String[] parts = val.split(":");
            if (parts.length > 2) {
                throw new IllegalArgumentException("Identifier '"+val+"' has too many parts; max one ':' symbol");
            }
            if (parts.length == 1 || Strings.isBlank(parts[1])) {
                throw new IllegalArgumentException("Identifier '"+val+"' must be of 'name:versionRange' syntax");
            } else if (singleVersionIsOsgiRange || (parts[1].startsWith("(") || parts[1].startsWith("["))) {
                return new VersionRangedName(parts[0], parts[1]);
            } else {
                return new VersionRangedName(parts[0], "["+parts[1]+","+parts[1]+"]");
            }
        }

        public VersionRangedName(String name, String v) {
            this.name = checkNotNull(name, "name");
            this.v = v;
        }
        public VersionRangedName(String name, @Nullable VersionRange v) {
            this.name = checkNotNull(name, "name").toString();
            this.v = v==null ? null : v.toString();
        }
        
        @Override
        public String toString() {
            return name + ":" + v;
        }
        
        public String toOsgiString() {
            return name + ":" + getOsgiVersionRange();
        }

        public boolean equals(String sn, String v) {
            return name.equals(sn) && Objects.equal(this.v, v);
        }

        public boolean equals(String sn, VersionRange v) {
            return name.equals(sn) && Objects.equal(getOsgiVersionRange(), v);
        }

        public String getSymbolicName() {
            return name;
        }

        private transient VersionRange cachedOsgiVersionRange;
        @Nullable
        public VersionRange getOsgiVersionRange() {
            if (cachedOsgiVersionRange==null && v!=null) {
                cachedOsgiVersionRange = v==null ? null : VersionRange.valueOf(BrooklynVersionSyntax.toValidOsgiVersionRange(v));
            }
            return cachedOsgiVersionRange;
        }

        @Nullable
        public String getOsgiVersionRangeString() {
            VersionRange ov = getOsgiVersionRange();
            if (ov==null) return null;
            return ov.toString();
        }

        @Nullable
        public String getVersionString() {
            return v;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(name, v);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof VersionRangedName)) {
                return false;
            }
            VersionRangedName o = (VersionRangedName) other;
            return Objects.equal(name, o.name) && Objects.equal(v, o.v);
        }
    }

    public static TypeUpgrades parseBundleManifestForTypeUpgrades(Bundle bundle) {
        Dictionary<String, String> headers = bundle.getHeaders();
        String removedLegacyItems = headers.get(MANIFEST_HEADER_REMOVE_LEGACY_ITEMS);
        if (removedLegacyItems != null) {
            List<VersionRangedName> versionedItems = parseVersionRangedNameList(removedLegacyItems, false);
            return new TypeUpgrades(versionedItems);
        }
        return TypeUpgrades.EMPTY;
    }
    
    @VisibleForTesting
    static List<VersionRangedName> parseVersionRangedNameList(String input, boolean singleVersionIsOsgiRange) {
        List<String> vals = QuotedStringTokenizer.builder()
                .delimiterChars(",")
                .includeQuotes(false)
                .includeDelimiters(false)
                .buildList(input);
        
        List<VersionRangedName> versionedItems = new ArrayList<>();
        for (String val : vals) {
            versionedItems.add(VersionRangedName.fromString(val, singleVersionIsOsgiRange));
        }
        return versionedItems;
    }
}
