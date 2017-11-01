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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.osgi.VersionedName.VersionedNameComparator;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.VersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

/**
 * Internal class for parsing bundle manifests to extract their upgrade instructions.
 */
public class BundleUpgradeParser {

    private static final Logger log = LoggerFactory.getLogger(BundleUpgradeParser.class);
    
    /**
     * A header in a bundle's manifest, indicating that this bundle will force the removal of the 
     * given legacy catalog items. Here "legacy" means those in the `/catalog` persisted state, 
     * rather than items added in bundles.
     * 
     * The format for the value is one of the following:
     * <ul>
     *   <li>Quoted {@code name:versionRange}, eg {@code "my-tomcat:[0,1)"};
     *       see {@link #MANIFEST_HEADER_FORCE_REMOVE_BUNDLES} for more information
     *   <li>{@code *} means all legacy items for things defined in this bundle, with version
     *       numbers lower than the version of the bundle,
     *       and quoted {@code *:versionRange} means the indicated version(s) of types in this bundle.
     *       For example, if the bundle is version 1.0.0 and its catalog.bom contains items "foo" and "bar", then 
     *       {@code *} is equivalent to writing {@code "foo:[0,1.0.0)","bar:[0,1.0.0)"}.
     *       As per the comments on {@link #MANIFEST_HEADER_FORCE_REMOVE_BUNDLES},
     *       the version of the bundle and items being added by a bundle to replace legacy catalog items
     *       should typically be larger in major/minor/point value,
     *       as a qualifier bump can be quite complex due to ordering differences.  
     *   <li>Comma-separated list of entries such as the above, eg {@code "my-tomcat:[0,1)","my-nginx:[0,1)"};
     *       see {@link #MANIFEST_HEADER_FORCE_REMOVE_BUNDLES} for more information
     * </ul>
     */
    @Beta
    public static final String MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS = "brooklyn-catalog-force-remove-legacy-items";

    /**
     * A header in a bundle's manifest, indicating that this bundle will force the removal of matching 
     * bundle(s) previously added and the types they contain.
     * 
     * The format for the value is one of:
     * <ul>
     *   <li>Quoted {@code name:versionRange}, where version range follows the OSGi conventions 
     *       (except that a single version number means exactly that version rather than greater 
     *       than or equal to that version). For example, {@code "org.example.mybundle:[0,1)"}.
     *       Note in particular this uses OSGi ordering semantics not Brooklyn ordering semantics,
     *       so qualifiers come <i>after</i> unqualified versions here, snapshot is not special-cased,
     *       and qualifiers (last/fourth segment) are compared alphabetically
     *       (thus "1.0" < "1.0.0.GA" < "1.0.0.SNAPSHOT" < "1.0.0.v10" < "1.0.0.v2" --
     *       but they are the same with respect to major/minor/point numbers so if in doubt stick with those!).
     *       Thus if using a range it is generally recommended to use a "[" square bracket start and ")" round bracket end
     *       so that the start is inclusive and end exclusive, and any edge cases explicitly referenced.
     *       If you want to replace a SNAPSHOT or RC version with a GA version you will need to call this out specially,
     *       as described in the "comma-separated list" format below.
     *       This is good anyway because there are different conventions for release names
     *       (e.g. "1.0.0" or "1.0.0.GA" or "1.0.0.2017-12") and any automation here is likely to cause surprises.
     *   <li>{@code *} as an entry, meaning all lower versions of this bundle,
     *       or quote {@code *:versionRange}, meaning the given version range on this bundle.
     *       For example, if the bundle is {@code org.example.mybundle:1.0.0}, 
     *       then {@code *} is equivalent to writing {@code "org.example.mybundle:[0,1.0.0)"}
     *       and {@code "*:1-SNAPSHOT"} is equivalent to writing {@code "org.example.mybundle:1-SNAPSHOT"}
     *       (which when converted to OSGi is equivalent to {@code "org.example.mybundle:1.0.0.SNAPSHOT"})
     *   <li>Comma-separated list of entries such as the above. For example,
     *       {@code "org.example.mybundle:[0,1)","org.example.myotherbundle:[0,1)"} (useful for
     *       when this bundle merges the contents of two previous bundles), or
     *       {@code "*","*:1.0.0.SNAPSHOT","*:1.0.0.rc1"}
     *       when releasing {@code org.example.mybundle:1.0.0.GA} 
     *       (to replace versions pre 1.0.0 as well as a snapshot and an rc1)
     * </ul>
     */
    @Beta
    public static final String MANIFEST_HEADER_FORCE_REMOVE_BUNDLES = "brooklyn-catalog-force-remove-bundles";

    /**
     * A header in a bundle's manifest, indicating that this bundle recommends a set of upgrades.
     * These will be advisory unless the bundle being upgraded is force-removed in which case it will be applied automatically
     * wherever the bundle is in use.
     * 
     * The format is a comma separate list of {@code key=value} pairs, where each key is a name with a version range
     * (as per {@link #MANIFEST_HEADER_FORCE_REMOVE_BUNDLES}) specifying what should be upgraded, and {@code value} is a name and
     * version specifying what it should be ugpraded to. The {@code =value} can be omitted, and usually is,
     * to mean this bundle at this version. (The {@code =value} is available if one bundle is defining upgrades for other bundles.)  
     * 
     * A wildcard can be given as the key, without a version ({@code *}) or with ({@code *:[0,1)}) to refer to 
     * all previous versions (OSGi ordering, see below) or the indicated versions, respectively, of the bundle this manifest is defining.
     * 
     * If this header is supplied and {@link #MANIFEST_HEADER_UPGRADE_FOR_TYPES} is omitted, the types to upgrade are inferred as 
     * being all types defined in the bundle this manifest is defining, and all versions of this bundle upgraded by this bundle
     * according to this header. If this header is included but does not declare any versions of this bundle upgraded by this bundle, 
     * then {@link #MANIFEST_HEADER_UPGRADE_FOR_TYPES} must be explicitly set.
     * 
     * Version ordering may be surprising particularly in regard to qualifiers.
     * See {@link #MANIFEST_HEADER_FORCE_REMOVE_BUNDLES} for discussion of this.
     * 
     * <p>
     * 
     * In most use cases, one can provide a single line, e.g. if releasing a v2.0.0:
     * 
     * {@code brooklyn-catalog-upgrade-for-bundles: *:[0,2)}
     * 
     * to indicate that this bundle is able to upgrade all instances of this bundle lower than 2.0.0,
     * and all types in this bundle will upgrade earlier types.
     * 
     * This can be used in conjunction with:
     * 
     * {@code brooklyn-catalog-force-remove-bundles: *:[0,2)}
     * 
     * to forcibly remove older bundles at an appropriate point (eg a restart) and cause all earlier instances of the bundle
     * and the type instances they contain to be upgraded with the same-named types in the 2.0.0 bundle.
     * 
     * It is not necessary to supply {@link #MANIFEST_HEADER_UPGRADE_FOR_TYPES} unless types are being renamed
     * or versions are not in line with previous versions of this bundle.
     */
    @Beta
    public static final String MANIFEST_HEADER_UPGRADE_FOR_BUNDLES = "brooklyn-catalog-upgrade-for-bundles";

    /**
     * A header in a bundle's manifest, indicating that this bundle recommends a set of upgrades.
     * These will be advisory unless the type being upgraded is force-removed in which case it will be applied automatically
     * wherever the type is in use.
     * 
     * The format is as per {@link #MANIFEST_HEADER_UPGRADE_FOR_BUNDLES}, with two differences in interpretation.
     * 
     * If {@code =value} is omitted, the upgrade target is assumed to be the same type as the corresponding key but at the
     * version of the bundle.
     * 
     * A wildcard can be given as the key, without a version ({@code *}) or with ({@code *:[0,1)}) to refer to
     * all types in the present bundle.
     * 
     * If the version/range for a type is omitted in any key, it is inferred as the versions of this bundle 
     * that this bundle declares it can upgrade in the {@link #MANIFEST_HEADER_UPGRADE_FOR_BUNDLES} header.
     * It is an error to omit a version/range if that header is not present or does not declare versions
     * of the same bundle being upgraded.  
     * 
     * If this key is omitted but a {@link #MANIFEST_HEADER_UPGRADE_FOR_BUNDLES} header is present defining 
     * versions of the containing bundle that are upgraded, then the default is {@code *} to give the common semantics
     * when a bundle is upgrading previous versions that types similarly upgrade.  If that header is absent or
     * this bundle is an upgrade for other bundles but not this bundle, then this key must be specified if
     * any types are intended to be upgraded.
     * 
     * What this is saying in most cases is that if a bundle {@code foo:1} contains {@code foo-node:1}, and 
     * bundle {@code foo:2} contains {@code foo-node:2}, then:
     * if {@code foo:2} declares {@code brooklyn-catalog-upgrade-for-bundles: foo:1} it will also declare that
     * {@code foo-node:2} upgrades {@code foo-node:1};
     * if {@code foo:2} declares {@code brooklyn-catalog-upgrade-for-bundles: *} the same thing will occur
     * (and it would also upgrade a {@code foo:0} contains {@code foo-node:0});
     * if {@code foo:2} declares no {@code brooklyn-catalog-upgrade} manifest headers, then no advisory
     * upgrades will be noted.
     * 
     * As noted in {@link #MANIFEST_HEADER_UPGRADE_FOR_BUNDLES} the primary use case for this header is type renames.
     */
    @Beta
    public static final String MANIFEST_HEADER_UPGRADE_FOR_TYPES = "brooklyn-catalog-upgrade-for-types";

    /**
     * The result from parsing bundle(s) to find their upgrade info.
     */
    public static class CatalogUpgrades {
        public static final CatalogUpgrades EMPTY = new CatalogUpgrades(builder());
        
        public static class Builder {
            private Set<VersionRangedName> removedLegacyItems = new LinkedHashSet<>();
            private Set<VersionRangedName> removedBundles = new LinkedHashSet<>();
            private Multimap<VersionedName,VersionRangedName> upgradesProvidedByBundles = LinkedHashMultimap.create();
            private Multimap<VersionedName,VersionRangedName> upgradesProvidedByTypes = LinkedHashMultimap.create();

            public Builder removedLegacyItems(Collection<VersionRangedName> vals) {
                removedLegacyItems.addAll(vals);
                return this;
            }
            public Builder removedBundles(Collection<VersionRangedName> vals) {
                removedBundles.addAll(vals);
                return this;
            }
            public Builder upgradeBundles(Multimap<VersionedName,VersionRangedName> vals) {
                upgradesProvidedByBundles.putAll(vals);
                return this;
            }
            public Builder upgradeTypes(Multimap<VersionedName,VersionRangedName> vals) {
                upgradesProvidedByTypes.putAll(vals);
                return this;
            }
            public Builder addAll(CatalogUpgrades other) {
                removedLegacyItems.addAll(other.removedLegacyItems);
                removedBundles.addAll(other.removedBundles);
                upgradesProvidedByBundles.putAll(other.upgradesProvidedByBundles);
                upgradesProvidedByTypes.putAll(other.upgradesProvidedByTypes);
                return this;
            }
            public CatalogUpgrades build() {
                return new CatalogUpgrades(this);
            }
            
            public Builder clearUpgradesProvidedByType(VersionedName versionedName) {
                upgradesProvidedByTypes.removeAll(versionedName);
                return this;
            }
            public Builder clearUpgradesProvidedByBundle(VersionedName versionedName) {
                upgradesProvidedByBundles.removeAll(versionedName);
                return this;
            }
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        private final Set<VersionRangedName> removedLegacyItems;
        private final Set<VersionRangedName> removedBundles;
        private final Multimap<VersionedName,VersionRangedName> upgradesProvidedByBundles;
        private final Multimap<VersionedName,VersionRangedName> upgradesProvidedByTypes;
        
        public CatalogUpgrades(Builder builder) {
            this.removedLegacyItems = ImmutableSet.copyOf(builder.removedLegacyItems);
            this.removedBundles = ImmutableSet.copyOf(builder.removedBundles);
            this.upgradesProvidedByBundles = ImmutableMultimap.copyOf(builder.upgradesProvidedByBundles);
            this.upgradesProvidedByTypes = ImmutableMultimap.copyOf(builder.upgradesProvidedByTypes);
        }

        public boolean isEmpty() {
            return removedLegacyItems.isEmpty() && removedBundles.isEmpty() && upgradesProvidedByBundles.isEmpty() && upgradesProvidedByTypes.isEmpty();
        }

        public Set<VersionRangedName> getRemovedLegacyItems() {
            return removedLegacyItems;
        }
        
        public Set<VersionRangedName> getRemovedBundles() {
            return removedBundles;
        }

        public Multimap<VersionedName, VersionRangedName> getUpgradesProvidedByBundles() {
            return upgradesProvidedByBundles;
        }
        
        public Multimap<VersionedName, VersionRangedName> getUpgradesProvidedByTypes() {
            return upgradesProvidedByTypes;
        }
        
        public boolean isLegacyItemRemoved(CatalogItem<?, ?> legacyCatalogItem) {
            VersionedName name = new VersionedName(legacyCatalogItem.getSymbolicName(), legacyCatalogItem.getVersion());
            return contains(removedLegacyItems, name);
        }

        public boolean isBundleRemoved(VersionedName bundle) {
            return contains(removedBundles, bundle);
        }
        
        public Set<VersionedName> getUpgradesForBundle(VersionedName bundle) {
            return findUpgradesIn(bundle, upgradesProvidedByBundles);
        }
        public Set<VersionedName> getUpgradesForType(VersionedName type) {
            return findUpgradesIn(type, upgradesProvidedByTypes);
        }
        private static Set<VersionedName> findUpgradesIn(VersionedName item, Multimap<VersionedName,VersionRangedName> upgradesMap) {
            Set<VersionedName> result = new TreeSet<>(VersionedNameComparator.INSTANCE);
            for (Map.Entry<VersionedName,VersionRangedName> n: upgradesMap.entries()) {
                if (contains(n.getValue(), item)) {
                    result.add(n.getKey());
                }
            }
            return result;
        }
        
        @Beta
        public static boolean contains(Iterable<VersionRangedName> names, VersionedName name) {
            for (VersionRangedName contender : names) {
                if (contains(contender, name)) {
                    return true;
                }
            }
            return false;
        }

        @Beta
        public static boolean contains(VersionRangedName range, VersionedName name) {
            return range.getSymbolicName().equals(name.getSymbolicName()) && range.getOsgiVersionRange().includes(name.getOsgiVersion());
        }

        @Beta
        public static void storeInManagementContext(CatalogUpgrades catalogUpgrades, ManagementContext managementContext) {
            ((BasicBrooklynTypeRegistry)managementContext.getTypeRegistry()).storeCatalogUpgradesInstructions(catalogUpgrades);
        }
        
        @Beta @Nonnull
        public static CatalogUpgrades getFromManagementContext(ManagementContext managementContext) {
            CatalogUpgrades result = ((BasicBrooklynTypeRegistry)managementContext.getTypeRegistry()).getCatalogUpgradesInstructions();
            if (result!=null) {
                return result;
            }
            return builder().build();
        }

        @Beta
        public static String getBundleUpgradedIfNecessary(ManagementContext mgmt, String vName) {
            if (vName==null) return null;
            Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgi.isAbsent()) {
                // ignore upgrades if not osgi
                return vName;
            }
            if (osgi.get().getManagedBundle(VersionedName.fromString(vName))!=null) {
                // bundle installed, prefer that to upgrade 
                return vName;
            }
            return getItemUpgradedIfNecessary(mgmt, vName, (cu,vn) -> cu.getUpgradesForBundle(vn));
        }
        @Beta
        public static String getTypeUpgradedIfNecessary(ManagementContext mgmt, String vName) {
            if (vName==null || mgmt.getTypeRegistry().get(vName)!=null) {
                // item found (or n/a), prefer that to upgrade
                return vName;
            }
            // callers should use BrooklynTags.newUpgradedFromTag if creating a spec,
            // then warn on instantiation, done only for entities currently.
            // (yoml will allow us to have one code path for all the different creation routines.)
            // persistence/rebind also warns.
            return getItemUpgradedIfNecessary(mgmt, vName, (cu,vn) -> cu.getUpgradesForType(vn));
        }
        
        private interface LookupFunction {
            public Set<VersionedName> lookup(CatalogUpgrades cu, VersionedName vn);
        }
        private static String getItemUpgradedIfNecessary(ManagementContext mgmt, String vName, LookupFunction lookupF) {
            Set<VersionedName> r = lookupF.lookup(getFromManagementContext(mgmt), VersionedName.fromString(vName));
            if (!r.isEmpty()) return r.iterator().next().toString();
            
            if (log.isTraceEnabled()) {
                log.trace("Could not find '"+vName+"' and no upgrades specified; subsequent failure or warning possible unless that is a direct java class reference");
            }
            return vName;
        }
        
        /** This method is used internally to mark places we need to update when we switch to persisting and loading
         *  registered type IDs instead of java types, as noted on RebindIteration.load */
        @Beta
        public static boolean markerForCodeThatLoadsJavaTypesButShouldLoadRegisteredType() {
            // return true if we use registered types, and update callers not to need this method
            return false;
        }
        
        @Beta
        CatalogUpgrades withTypeCleared(VersionedName versionedName) {
            return builder().addAll(this).clearUpgradesProvidedByType(versionedName).build();
        }
        @Beta
        CatalogUpgrades withBundleCleared(VersionedName versionedName) {
            return builder().addAll(this).clearUpgradesProvidedByType(versionedName).build();
        }

        @Beta
        public static void clearTypeInStoredUpgrades(ManagementContext mgmt, VersionedName versionedName) {
            synchronized (mgmt.getTypeRegistry()) {
                storeInManagementContext(getFromManagementContext(mgmt).withTypeCleared(versionedName), mgmt);
            }
        }
        @Beta
        public static void clearBundleInStoredUpgrades(ManagementContext mgmt, VersionedName versionedName) {
            synchronized (mgmt.getTypeRegistry()) {
                storeInManagementContext(getFromManagementContext(mgmt).withBundleCleared(versionedName), mgmt);
            }
        }
    }
    
    /**
     * Records a name (string) and version range (string),
     * with conveniences for pretty-printing and converting to OSGi format.
     * 
     * Implementation-wise, this is similar to {@link VersionedName}, but is intended
     * as internal-only so is cut down to only what is needed.
     * 
     * Both the name and the version range are required.
     */
    public static class VersionRangedName {
        private final String name;
        private final String v;
        private transient volatile VersionRange cachedOsgiVersionRange;

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
            }
            return new VersionRangedName(parts[0], parts[1], singleVersionIsOsgiRange);
        }

        protected static String tidyVersionRange(String v, boolean singleVersionIsOsgiRange) {
            if (v==null) return null;
            if (singleVersionIsOsgiRange || (v.startsWith("(") || v.startsWith("["))) {
                return v;
            } else {
                return "["+v+","+v+"]";
            }
        }

        public VersionRangedName(String name, VersionRange v) {
            this.name = checkNotNull(name, "name").toString();
            this.v = checkNotNull(v, "versionRange").toString();
        }
        
        private VersionRangedName(String name, String v, boolean singleVersionIsOsgiRange) {
            this.name = checkNotNull(name, "name");
            this.v = tidyVersionRange(checkNotNull(v, "versionRange"), singleVersionIsOsgiRange); 
        }
        
        @Override
        public String toString() {
            return name + ":" + v;
        }
        
        public String toOsgiString() {
            return name + ":" + getOsgiVersionRange();
        }

        public String getSymbolicName() {
            return name;
        }

        public VersionRange getOsgiVersionRange() {
            if (cachedOsgiVersionRange == null) {
                cachedOsgiVersionRange = VersionRange.valueOf(BrooklynVersionSyntax.toValidOsgiVersionRange(v));
            }
            return cachedOsgiVersionRange;
        }

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

    public static CatalogUpgrades parseBundleManifestForCatalogUpgrades(Bundle bundle, Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier) {
        // TODO Add support for the other options described in the proposal:
        //   https://docs.google.com/document/d/1Lm47Kx-cXPLe8BO34-qrL3ZMPosuUHJILYVQUswEH6Y/edit#
        //   section "Bundle Upgrade Metadata"
        
        Dictionary<String, String> headers = bundle.getHeaders();
        String upgradesForBundlesHeader = headers.get(MANIFEST_HEADER_UPGRADE_FOR_BUNDLES);
        Multimap<VersionedName,VersionRangedName> upgradesForBundles = parseUpgradeForBundlesHeader(upgradesForBundlesHeader, bundle);
        return CatalogUpgrades.builder()
                .removedLegacyItems(parseForceRemoveLegacyItemsHeader(headers.get(MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS), bundle, typeSupplier))
                .removedBundles(parseForceRemoveBundlesHeader(headers.get(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES), bundle))
                .upgradeBundles(upgradesForBundles)
                .upgradeTypes(parseUpgradeForTypesHeader(headers.get(MANIFEST_HEADER_UPGRADE_FOR_TYPES), bundle, typeSupplier, 
                    upgradesForBundlesHeader==null ? null : upgradesForBundles))
                .build();
    }

    @VisibleForTesting
    static List<VersionRangedName> parseForceRemoveLegacyItemsHeader(String input, Bundle bundle, Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier) {
        return parseVersionRangedNameList(input, false, getTypeNamesInBundle(typeSupplier), "[0,"+bundle.getVersion()+")");
    }
    
    @VisibleForTesting
    static List<VersionRangedName> parseForceRemoveBundlesHeader(String input, Bundle bundle) {
        if (input == null) return ImmutableList.of();
        return parseVersionRangedNameList(input, false, MutableList.of(bundle.getSymbolicName()), getDefaultSourceVersionRange(bundle));
    }

    private static Multimap<VersionedName, VersionRangedName> parseUpgradeForBundlesHeader(String input, Bundle bundle) {
        return parseVersionRangedNameEqualsVersionedNameList(input, false,
            // wildcard means this bundle, all lower versions
            MutableList.of(bundle.getSymbolicName()), MutableList.of(getDefaultSourceVersionRange(bundle)),
            // default target is this bundle
            (i) -> { return new VersionedName(bundle); });
    }
    private static Multimap<VersionedName, VersionRangedName> parseUpgradeForTypesHeader(String input, Bundle bundle, Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier, Multimap<VersionedName, VersionRangedName> upgradesForBundles) {
        List<String> sourceVersions = null;
        if (upgradesForBundles!=null) {
            Collection<VersionRangedName> acceptableRanges = upgradesForBundles.get(new VersionedName(bundle));
            if (acceptableRanges!=null && !acceptableRanges.isEmpty()) {
                for (VersionRangedName n: acceptableRanges) {
                    if (n.getSymbolicName().equals(bundle.getSymbolicName())) {
                        if (sourceVersions==null) {
                            sourceVersions = MutableList.of();
                        }
                        sourceVersions.add(n.getOsgiVersionRange().toString());
                    }
                }
            }
        }
        Set<VersionedName> typeSupplierNames = MutableList.copyOf(typeSupplier.get()).stream().map(
            (t) -> VersionedName.toOsgiVersionedName(t.getVersionedName())).collect(Collectors.toSet());
        if (input==null && sourceVersions!=null && !sourceVersions.isEmpty()) {
            input = "*";
        }
        return parseVersionRangedNameEqualsVersionedNameList(input, false,
            // wildcard means all types, all versions of this bundle this bundle replaces
            getTypeNamesInBundle(typeSupplier), sourceVersions,
            // default target is same type at version of this bundle
            (i) -> { 
                VersionedName targetTypeAtBundleVersion = new VersionedName(i.getSymbolicName(), bundle.getVersion());
                if (!typeSupplierNames.contains(VersionedName.toOsgiVersionedName(targetTypeAtBundleVersion))) {
                    throw new IllegalStateException("Bundle manifest declares it upgrades "+i+" "
                        + "but does not declare an explicit target and does not contain inferred target "+targetTypeAtBundleVersion);
                }
                return targetTypeAtBundleVersion; 
            });
    }
    
    @VisibleForTesting
    static List<String> getTypeNamesInBundle(Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier) {
        List<String> wildcardItems = MutableList.of();
        for (RegisteredType item : typeSupplier.get()) {
            wildcardItems.add(item.getSymbolicName());
        }
        return wildcardItems;
    }
    
    @VisibleForTesting
    static String getDefaultSourceVersionRange(Bundle bundle) {
        String bundleVersion = bundle.getVersion().toString();
        String maxVersion;
        if (BrooklynVersionSyntax.isSnapshot(bundleVersion)) {
            maxVersion = BrooklynVersionSyntax.stripSnapshot(bundleVersion);
        } else {
            maxVersion = bundleVersion;
        }
        String defaultSourceVersion = "[0,"+maxVersion+")";
        return defaultSourceVersion;
    }
    
    @VisibleForTesting
    static List<VersionRangedName> parseVersionRangedNameList(String input, boolean singleVersionIsOsgiRange,
            List<String> wildcardNames, String wildcardVersion) {
        if (input == null) return ImmutableList.of();
        
        List<String> vals = QuotedStringTokenizer.builder()
                .delimiterChars(",")
                .includeQuotes(false)
                .includeDelimiters(false)
                .buildList(input);
        
        List<VersionRangedName> versionedItems = new ArrayList<>();
        for (String val : vals) {
            val = val.trim();
            if (val.startsWith("*")) {
                String r;
                if ("*".equals(val)) {
                    r = wildcardVersion;
                } else if (val.startsWith("*:")) {
                    r = val.substring(2);
                } else {
                    throw new IllegalArgumentException("Wildcard entry must be of the form \"*\" or \"*:range\"");
                }
                for (String item: wildcardNames) {
                    versionedItems.add(parseVersionRangedName(item, r, false));
                }
            } else {
                versionedItems.add(parseVersionRangedName(val, singleVersionIsOsgiRange));
            }
        }
        return versionedItems;
    }

        private static VersionRangedName parseVersionRangedName(String val, boolean singleVersionIsOsgiRange) {
            try {
                return VersionRangedName.fromString(val, singleVersionIsOsgiRange);
            } catch (Exception e) {
                if (Strings.containsAny(val, "(", ")", "[", "]") &&
                        !Strings.containsAny(val, "'", "\"")) {
                    throw Exceptions.propagateAnnotated("Entry cannot be parsed. If defining a range on an entry you must quote the entry.", e);
                }
                throw Exceptions.propagate(e);
            }
        }
        
        private static VersionRangedName parseVersionRangedName(String name, String range, boolean singleVersionIsOsgiRange) {
            try {
                return new VersionRangedName(name, range, singleVersionIsOsgiRange);
            } catch (Exception e) {
                if (Strings.containsAny(range, "(", ")", "[", "]") &&
                        !Strings.containsAny(range, "'", "\"")) {
                    throw Exceptions.propagateAnnotated("Entry cannot be parsed. If defining a range on an entry you must quote the entry.", e);
                }
                throw Exceptions.propagate(e);
            }
        }
        
    @VisibleForTesting
    static Multimap<VersionedName,VersionRangedName> parseVersionRangedNameEqualsVersionedNameList(String input, boolean singleVersionIsOsgiRange,
            List<String> wildcardNames, List<String> wildcardVersions,
            Function<VersionRangedName,VersionedName> defaultTargetFunction) {
        LinkedHashMultimap<VersionedName,VersionRangedName> result = LinkedHashMultimap.create();
        if (input == null) return result;
        
        List<String> vals = QuotedStringTokenizer.builder()
                .delimiterChars(",")
                .includeQuotes(false)
                .includeDelimiters(false)
                .buildList(input);
        
        for (String entry : vals) {
            entry = entry.trim();
            String key, val;
            String[] keVals = entry.split("=");
            if (keVals.length>2) {
                throw new IllegalArgumentException("Max one = permitted in entry (\""+entry+"\"). If defining a range on an entry you must quote the entry.");
            } else if (keVals.length==2) {
                key = keVals[0];
                val = keVals[1];
            } else {
                key = keVals[0];
                val = null;
            }
            
            List<String> sourceNames, sourceVersions;
            if (key.startsWith("*")) {
                if (wildcardNames==null) {
                    throw new IllegalArgumentException("Wildcard cannot be inferred");
                }
                if  ("*".equals(key)) {
                    if (wildcardVersions==null) {
                        throw new IllegalArgumentException("Version for wildcard cannot be inferred");
                    }
                    sourceVersions = wildcardVersions;
                } else if (key.startsWith("*:")) {
                    sourceVersions = MutableList.of(key.substring(2));
                } else {
                    throw new IllegalArgumentException("Wildcard entry key must be of the form \"*\" or \"*:range\"");
                }
                sourceNames = MutableList.copyOf(wildcardNames);
            } else {
                String[] parts = key.split(":");
                if (parts.length==1) {
                    if (wildcardVersions==null) {
                        throw new IllegalArgumentException("Version for "+key+" cannot be inferred");
                    }
                    sourceNames = MutableList.of(key);
                    sourceVersions = wildcardVersions;
                } else if (parts.length==2) {
                    sourceNames = MutableList.of(parts[0]);
                    sourceVersions = MutableList.of(parts[1]);
                } else {
                    throw new IllegalArgumentException("Entry '"+entry+"' should be of form 'name[:versionRange][=name[:version]]'");
                }
            }
            for (String item: sourceNames) {
                for (String v: sourceVersions) {
                    VersionRangedName source = parseVersionRangedName(item, v, false);
                    VersionedName target;
                    if (val!=null) {
                        target = VersionedName.fromString(val);
                    } else if (defaultTargetFunction!=null) {
                        target = defaultTargetFunction.apply(source);
                    } else {
                        throw new IllegalArgumentException("Wildcard entry key must be of the form \"*\" or \"*:range\"");
                    }
                    result.put(target, source);
                }
            }
        }
        return result;
    }
    
    @VisibleForTesting
    static String stripQuotes(String input) {
        String quoteChars = QuotedStringTokenizer.DEFAULT_QUOTE_CHARS;
        boolean quoted = (input.length() >= 2) && quoteChars.contains(input.substring(0, 1))
                && quoteChars.contains(input.substring(input.length() - 1));
        return (quoted ? input.substring(1, input.length() - 1) : input);
    }
    
}