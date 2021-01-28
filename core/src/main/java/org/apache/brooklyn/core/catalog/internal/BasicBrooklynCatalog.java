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

import com.google.common.base.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.reflect.TypeToken;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.CatalogClasspathDo.CatalogScanningModes;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.CampYamlParser;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.*;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution.BrooklynTypeNameResolver;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.apache.brooklyn.util.javalang.AggregateClassLoader;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.LoadedClassLoader;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.InputStreamSource;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yaml.Yamls.YamlExtract;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/* TODO the complex tree-structured catalogs are only useful when we are relying on those separate catalog classloaders
 * to isolate classpaths. with osgi everything is just put into the "manual additions" catalog. Deprecate/remove this. */
public class BasicBrooklynCatalog implements BrooklynCatalog {
    public static final String POLICIES_KEY = "brooklyn.policies";
    public static final String ENRICHERS_KEY = "brooklyn.enrichers";
    public static final String LOCATIONS_KEY = "brooklyn.locations";
    public static final String NO_VERSION = "0.0.0-SNAPSHOT";

    public static final String CATALOG_BOM = "catalog.bom";
    // should always be 1.0; see bottom of
    // http://www.eclipse.org/virgo/documentation/virgo-documentation-3.7.0.M01/docs/virgo-user-guide/html/ch02s02.html
    // (some things talk of 2.0, but haven't investigated that)
    public static final String OSGI_MANIFEST_VERSION_VALUE = "1.0";

    /** Header on bundle indicating it is a wrapped BOM with no other resources */
    public static final String BROOKLYN_WRAPPED_BOM_BUNDLE = "Brooklyn-Wrapped-BOM";
    @VisibleForTesting
    public static final boolean AUTO_WRAP_CATALOG_YAML_AS_BUNDLE = true;
    
    private static final Logger log = LoggerFactory.getLogger(BasicBrooklynCatalog.class);

    public static class BrooklynLoaderTracker {
        public static final ThreadLocal<BrooklynClassLoadingContext> loader = new ThreadLocal<BrooklynClassLoadingContext>();
        
        public static void setLoader(BrooklynClassLoadingContext val) {
            loader.set(val);
        }
        
        // If needed, could use stack; see ClassLoaderFromStack...
        public static void unsetLoader(BrooklynClassLoadingContext val) {
            loader.set(null);
        }
        
        public static BrooklynClassLoadingContext getLoader() {
            return loader.get();
        }
    }

    private final ManagementContext mgmt;
    private CatalogDo catalog;
    private volatile CatalogDo manualAdditionsCatalog;
    private volatile LoadedClassLoader manualAdditionsClasses;
    private final AggregateClassLoader rootClassLoader = AggregateClassLoader.newInstanceWithNoLoaders();
    
    /**
     * Cache of specs (used by {@link #peekSpec(CatalogItem)}).
     * We assume that no-one is modifying the catalog items (once added) without going through the
     * correct accessor methods here (e.g. no-one calling {@code getCatalogItemDo().getDto().setXyz()}).
     * 
     * As discussed in https://github.com/apache/brooklyn-server/pull/423 and BROOKLYN-382, there  
     * are things outside of the control of the catalog that a spec depends on - like non-catalog 
     * locations, type registry, adding bundles, etc. However, because this cache is only used for
     * {@link #peekSpec(CatalogItem)}, it is considered good enough.
     * 
     * A longer term improvement is to focus on our YAML parsing, to make that faster and better!
     */
    private final SpecCache specCache;

    public BasicBrooklynCatalog(ManagementContext mgmt) {
        this(mgmt, CatalogDto.newNamedInstance("empty catalog", "empty catalog", "empty catalog, expected to be reset later"));
    }

    public BasicBrooklynCatalog(ManagementContext mgmt, CatalogDto dto) {
        this.mgmt = checkNotNull(mgmt, "managementContext");
        this.catalog = new CatalogDo(mgmt, dto);
        this.specCache = new SpecCache();
    }

    public boolean blockIfNotLoaded(Duration timeout) {
        try {
            return getCatalog().blockIfNotLoaded(timeout);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    public void reset(CatalogDto dto) {
        reset(dto, true);
    }

    public void reset(CatalogDto dto, boolean failOnLoadError) {
        specCache.invalidate();
        // Unregister all existing persisted items.
        for (CatalogItem<?, ?> toRemove : getCatalogItemsLegacy()) {
            if (log.isTraceEnabled()) {
                log.trace("Scheduling item for persistence removal: {}", toRemove.getId());
            }
            mgmt.getRebindManager().getChangeListener().onUnmanaged(toRemove);
        }
        CatalogDo catalog = new CatalogDo(mgmt, dto);
        CatalogUtils.logDebugOrTraceIfRebinding(log, "Resetting "+this+" catalog to "+dto);
        catalog.load(mgmt, null, failOnLoadError);
        CatalogUtils.logDebugOrTraceIfRebinding(log, "Reloaded catalog for "+this+", now switching");
        this.catalog = catalog;
        resetRootClassLoader();
        this.manualAdditionsCatalog = null;
        
        // Inject management context into and persist all the new entries.
        for (CatalogItem<?, ?> entry : getCatalogItemsLegacy()) {
            boolean setManagementContext = false;
            if (entry instanceof CatalogItemDo) {
                CatalogItemDo<?, ?> cid = CatalogItemDo.class.cast(entry);
                if (cid.getDto() instanceof CatalogItemDtoAbstract) {
                    CatalogItemDtoAbstract<?, ?> cdto = CatalogItemDtoAbstract.class.cast(cid.getDto());
                    if (cdto.getManagementContext() == null) {
                        cdto.setManagementContext((ManagementContextInternal) mgmt);
                    }
                    setManagementContext = true;
                    onAdditionUpdateOtherRegistries(cdto);
                }
            }
            if (!setManagementContext) {
                log.warn("Can't set management context on entry with unexpected type in catalog. type={}, " +
                        "expected={}", entry, CatalogItemDo.class);
            }
            if (log.isTraceEnabled()) {
                log.trace("Scheduling item for persistence addition: {}", entry.getId());
            }
            
            mgmt.getRebindManager().getChangeListener().onManaged(entry);
        }

    }

    /**
     * Resets the catalog to the given entries
     */
    @Override
    public void reset(Collection<CatalogItem<?, ?>> entries) {
        CatalogDto newDto = CatalogDto.newDtoFromCatalogItems(entries, "explicit-catalog-reset");
        reset(newDto);
    }
    
    public CatalogDo getCatalog() {
        return catalog;
    }

    protected CatalogItemDo<?,?> getCatalogItemDo(String symbolicName, String version) {
        String fixedVersionId = getFixedVersionId(symbolicName, version);
        if (fixedVersionId == null) {
            //no items with symbolicName exist
            return null;
        }

        return catalog.getIdCache().get( CatalogUtils.getVersionedId(symbolicName, fixedVersionId) );
    }
    
    private String getFixedVersionId(String symbolicName, String version) {
        if (version!=null && !DEFAULT_VERSION.equals(version)) {
            return version;
        } else {
            return getBestVersion(symbolicName);
        }
    }

    /** returns best version, as defined by {@link BrooklynCatalog#getCatalogItem(String, String)} */
    private String getBestVersion(String symbolicName) {
        Iterable<CatalogItem<Object, Object>> versions = getCatalogItems(Predicates.and(
                CatalogPredicates.disabled(false),
                CatalogPredicates.symbolicName(Predicates.equalTo(symbolicName))));
        Collection<CatalogItem<Object, Object>> orderedVersions = sortVersionsDesc(versions);
        if (!orderedVersions.isEmpty()) {
            return orderedVersions.iterator().next().getVersion();
        } else {
            return null;
        }
    }

    private <T,SpecT> Collection<CatalogItem<T,SpecT>> sortVersionsDesc(Iterable<CatalogItem<T,SpecT>> versions) {
        return ImmutableSortedSet.orderedBy(CatalogItemComparator.<T,SpecT>getInstance()).addAll(versions).build();
    }

    @Override @Deprecated
    public CatalogItem<?,?> getCatalogItem(String symbolicName, String version) {
        CatalogItem<?,?> legacy = getCatalogItemLegacy(symbolicName, version);
        if (legacy!=null) return legacy;
        RegisteredType rt = mgmt.getTypeRegistry().get(symbolicName, version);
        if (rt!=null) return RegisteredTypes.toPartialCatalogItem(rt);
        return null;
    }
    @Override @Deprecated
    public CatalogItem<?,?> getCatalogItemLegacy(String symbolicName, String version) {
        if (symbolicName == null) return null;
        CatalogItemDo<?, ?> itemDo = getCatalogItemDo(symbolicName, version);
        if (itemDo == null) return null;
        return itemDo.getDto();
    }
    
    private static ThreadLocal<Boolean> deletingCatalogItem = new ThreadLocal<>();
    @Override @Deprecated
    public void deleteCatalogItem(String symbolicName, String version) {
        deleteCatalogItem(symbolicName, version, true, true);
    }
    @Override @Deprecated
    public void deleteCatalogItem(String symbolicName, String version, boolean alsoCheckTypeRegistry, boolean failIfNotFound) {
        if (alsoCheckTypeRegistry && !Boolean.TRUE.equals(deletingCatalogItem.get())) {
            // while we switch from catalog to type registry, make sure deletion covers both;
            // thread local lets us call to other once then he calls us and we do other code path
            deletingCatalogItem.set(true);
            try {
                RegisteredType item = mgmt.getTypeRegistry().get(symbolicName, version);
                if (item==null) {
                    log.debug("Request to delete "+symbolicName+":"+version+" but nothing matching found; ignoring");
                } else {
                    ((BasicBrooklynTypeRegistry) mgmt.getTypeRegistry()).delete( item );
                }
                return;
            } finally {
                deletingCatalogItem.remove();
            }
        }
        
        log.debug("Deleting manual catalog item from "+mgmt+": "+symbolicName + ":" + version);
        checkNotNull(symbolicName, "id");
        checkNotNull(version, "version");
        if (DEFAULT_VERSION.equals(version)) {
            throw new IllegalStateException("Deleting items with unspecified version (argument DEFAULT_VERSION) not supported.");
        }
        CatalogItem<?, ?> item = getCatalogItemLegacy(symbolicName, version);
        CatalogItemDtoAbstract<?,?> itemDto = getAbstractCatalogItem(item);
        if (itemDto == null) {
            if (failIfNotFound) {
                throw new NoSuchElementException("No catalog item found with id "+symbolicName);
            } else {
                return;
            }
        }
        if (manualAdditionsCatalog==null) loadManualAdditionsCatalog();
        manualAdditionsCatalog.deleteEntry(itemDto);
        
        // Ensure the caches are de-populated
        specCache.invalidate();
        getCatalog().deleteEntry(itemDto);

        // And indicate to the management context that it should be removed.
        if (log.isTraceEnabled()) {
            log.trace("Scheduling item for persistence removal: {}", itemDto.getId());
        }
        mgmt.getRebindManager().getChangeListener().onUnmanaged(itemDto);

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <T,SpecT> CatalogItem<T,SpecT> getCatalogItem(Class<T> type, String id, String version) {
        CatalogItem<T, SpecT> item = (CatalogItem) getCatalogItemLegacy(type, id, version);
        if (item!=null) {
            return item;
        }
        
        RegisteredType rt = mgmt.getTypeRegistry().get(id, version);
        if (rt!=null) {
            if (rt.getSuperTypes().contains(type) || rt.getSuperTypes().contains(type.getName())) {
                return (CatalogItem) RegisteredTypes.toPartialCatalogItem(rt);
            }
        }
        
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T,SpecT> CatalogItem<T,SpecT> getCatalogItemLegacy(Class<T> type, String id, String version) {
        if (id==null || version==null) return null;
        CatalogItem<?,?> result = getCatalogItem(id, version);
        if (result==null) return null;
        if (type==null || type.isAssignableFrom(result.getCatalogItemJavaType())) 
            return (CatalogItem<T,SpecT>)result;
        return null;
    }

    @Override
    public void persist(CatalogItem<?, ?> catalogItem) {
        checkArgument(getCatalogItem(catalogItem.getSymbolicName(), catalogItem.getVersion()) != null, "Unknown catalog item %s", catalogItem);
        mgmt.getRebindManager().getChangeListener().onChanged(catalogItem);
    }
    
    @Override
    public ClassLoader getRootClassLoader() {
        if (rootClassLoader.isEmpty() && catalog!=null) {
            resetRootClassLoader();
        }
        return rootClassLoader;
    }

    private void resetRootClassLoader() {
        specCache.invalidate();
        rootClassLoader.reset(ImmutableList.of(catalog.getRootClassLoader()));
    }

    /**
     * Loads this catalog. No effect if already loaded.
     */
    public void load() {
        log.debug("Loading catalog for " + mgmt);
        getCatalog().load(mgmt, null);
        if (log.isDebugEnabled()) {
            log.debug("Loaded catalog for " + mgmt + ": " + catalog + "; search classpath is " + catalog.getRootClassLoader());
        }
    }

    @Override
    public AbstractBrooklynObjectSpec<?, ?> peekSpec(CatalogItem<?, ?> item) {
        if (item == null) return null;
        CatalogItemDo<?, ?> loadedItem = getCatalogItemDo(item.getSymbolicName(), item.getVersion());
        if (loadedItem == null) throw new RuntimeException(item+" not in catalog; cannot create spec");
        if (loadedItem.getSpecType()==null) return null;
        String itemId = item.getCatalogItemId();
        
        Optional<AbstractBrooklynObjectSpec<?, ?>> cachedSpec = specCache.getSpec(itemId);
        if (cachedSpec.isPresent()) {
            return cachedSpec.get();
        } else {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            AbstractBrooklynObjectSpec<?, ?> spec = internalCreateSpecLegacy(mgmt, (CatalogItem)loadedItem, MutableSet.<String>of(), true);
            if (spec != null) {
                specCache.addSpec(itemId, spec);
                return spec;
            }
        }

        throw new IllegalStateException("No known mechanism to create instance of "+item);
    }
    
    @Override
    @Deprecated
    @SuppressWarnings("unchecked")
    public <T, SpecT extends AbstractBrooklynObjectSpec<? extends T, SpecT>> SpecT createSpec(CatalogItem<T, SpecT> item) {
        if (item == null) return null;
        CatalogItemDo<T,SpecT> loadedItem = (CatalogItemDo<T, SpecT>) getCatalogItemDo(item.getSymbolicName(), item.getVersion());

        if (loadedItem == null) {
            RegisteredType registeredType = mgmt.getTypeRegistry().get(item.getSymbolicName(), item.getVersion());
            if(registeredType == null) {
                throw new RuntimeException(item + " not in catalog; cannot create spec");
            }

            AbstractBrooklynObjectSpec<?, ?> spec = mgmt.getTypeRegistry().createSpec(registeredType, null, null);
            if(spec == null) {
                throw new RuntimeException("Problem loading spec for type "+registeredType);
            }

            return (SpecT)spec;
        }

        if (loadedItem.getSpecType()==null) return null;
        
        SpecT spec = internalCreateSpecLegacy(mgmt, loadedItem, MutableSet.<String>of(), true);
        if (spec != null) {
            return spec;
        }
        
        throw new IllegalStateException("No known mechanism to create instance of "+item);
    }
    
    /** @deprecated since introduction in 0.9.0, only used for backwards compatibility, can be removed any time;
     * uses the type-creation info on the item.
     * deprecated transformers must be included by routines which don't use {@link BrooklynTypePlanTransformer} instances;
     * otherwise deprecated transformers should be excluded. (deprecation is taken as equivalent to having a new-style transformer.) */
    @Deprecated 
    public static <T,SpecT extends AbstractBrooklynObjectSpec<? extends T, SpecT>> SpecT internalCreateSpecLegacy(ManagementContext mgmt, final CatalogItem<T, SpecT> item, final Set<String> encounteredTypes, boolean includeDeprecatedTransformers) {
        // deprecated lookup
        if (encounteredTypes.contains(item.getSymbolicName())) {
            throw new IllegalStateException("Type being resolved '"+item.getSymbolicName()+"' has already been encountered in " + encounteredTypes + "; recursive cycle detected");
        }
        Maybe<SpecT> specMaybe = org.apache.brooklyn.core.plan.PlanToSpecFactory.attemptWithLoaders(mgmt, includeDeprecatedTransformers, new Function<org.apache.brooklyn.core.plan.PlanToSpecTransformer, SpecT>() {
            @Override
            public SpecT apply(org.apache.brooklyn.core.plan.PlanToSpecTransformer input) {
                return input.createCatalogSpec(item, encounteredTypes);
            }
        });
        return specMaybe.get();
    }

    @Deprecated /** @deprecated since 0.7.0 only used by other deprecated items */ 
    private <T,SpecT> CatalogItemDtoAbstract<T,SpecT> getAbstractCatalogItem(CatalogItem<T,SpecT> item) {
        while (item instanceof CatalogItemDo) item = ((CatalogItemDo<T,SpecT>)item).itemDto;
        if (item==null) return null;
        if (item instanceof CatalogItemDtoAbstract) return (CatalogItemDtoAbstract<T,SpecT>) item;
        CatalogItem<?, ?> item2 = getCatalogItemLegacy(item.getSymbolicName(), item.getVersion());
        if (item2 instanceof CatalogItemDtoAbstract) return (CatalogItemDtoAbstract<T,SpecT>) item2;
        throw new IllegalStateException("Cannot unwrap catalog item '"+item+"' (type "+item.getClass()+") to restore DTO");
    }
    
    @SuppressWarnings("unchecked")
    private static <T> Maybe<T> getFirstAs(Map<?,?> map, Class<T> type, String firstKey, String ...otherKeys) {
        return ConfigUtils.getFirstAs(map, type, firstKey, otherKeys);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Maybe<Map<?,?>> getFirstAsMap(Map<?,?> map, String firstKey, String ...otherKeys) {
        return (Maybe) getFirstAs(map, Map.class, firstKey, otherKeys);
    }

    public static Map<?,?> getCatalogMetadata(String yaml) {
        Map<?,?> itemDef = Yamls.getAs(Yamls.parseAll(yaml), Map.class);
        return getFirstAsMap(itemDef, "brooklyn.catalog").orNull();        
    }
    
    public static VersionedName getVersionedName(Map<?,?> catalogMetadata, boolean required) {
        // for better legacy compatibility, if id specified at root use that for bundle symbolic name and optionally for version
        VersionedName idV = null;
        String idS = getFirstAs(catalogMetadata, String.class, "id").orNull();
        if (Strings.isNonBlank(idS)) {
            idV = VersionedName.fromString(idS);
        }

        String version = getFirstAs(catalogMetadata, String.class, "version").orNull();
        String bundle = getFirstAs(catalogMetadata, String.class, "bundle").orNull();
        if (bundle==null) {
            // if bundle not specified, ID indicates bundle name, and version if specified must match
            if (idV!=null) {
                bundle = idV.getSymbolicName();
                if (Strings.isNonBlank(idV.getVersionString())) {
                    if (version!=null) {
                        if (!Objects.equal(version, idV.getVersionString()) && !Objects.equal(version, idV.getOsgiVersionString())) {
                            throw new IllegalStateException("Catalog BOM using ID '" + idV + "' to define bundle does not match declared version '" + version + "'");
                        }
                    } else {
                        version = idV.getVersionString();
                    }
                } else {
                    if (required) {
                        throw new IllegalStateException("Catalog BOM must define bundle name and version or include version as part of the id '" + bundle + "' (eg '" + bundle + ":1.0')");
                    } else {
                        // allow null version
                    }
                }
            }
        }

        if (Strings.isBlank(bundle) && Strings.isBlank(version)) {
            if (!required) return null;
            throw new IllegalStateException("Catalog BOM must define bundle name and version");
        }
        if (Strings.isBlank(bundle)) {
            if (!required) return null;
            throw new IllegalStateException("Catalog BOM must define bundle (or id)");
        }
        if (Strings.isBlank(version)) {
            if (required) {
                throw new IllegalStateException("Catalog BOM must define version where bundle name '" + bundle + "' is defined");
            }
        }

        return new VersionedName(bundle, version);
    }

    /** See comments on {@link #collectCatalogItemsFromItemMetadataBlock(String, ManagedBundle, Map, List, Map, boolean, Map, int, boolean, Boolean)};
     * this is a shell around that that parses the `brooklyn.catalog` header on the BOM YAML file */
    private void collectCatalogItemsFromCatalogBomRoot(String contextForError, String yaml, ManagedBundle containingBundle, 
            List<CatalogItemDtoAbstract<?, ?>> resultLegacyFormat, Map<RegisteredType, RegisteredType> resultNewFormat, 
            boolean requireValidation, Map<?, ?> parentMeta, int depth, boolean force, Boolean throwOnError) {
        Map<?,?> itemDef;
        try {
            itemDef = Yamls.getAs(Yamls.parseAll(yaml), Map.class);
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Error parsing YAML in "+contextForError, e);
        }
        Map<?,?> catalogMetadata = getFirstAsMap(itemDef, "brooklyn.catalog").orNull();
        if (catalogMetadata==null)
            log.warn("No `brooklyn.catalog` supplied in catalog request; using legacy mode for "+itemDef);
        catalogMetadata = MutableMap.copyOf(catalogMetadata);

        collectCatalogItemsFromItemMetadataBlock(Yamls.getTextOfYamlAtPath(yaml, "brooklyn.catalog").getMatchedYamlTextOrWarn(), 
            containingBundle, catalogMetadata, resultLegacyFormat, resultNewFormat, requireValidation, parentMeta, 0, force, throwOnError);
        
        itemDef.remove("brooklyn.catalog");
        catalogMetadata.remove("item");
        catalogMetadata.remove("items");
        if (!itemDef.isEmpty()) {
            // AH - i forgot we even supported this. probably no point anymore,
            // now that catalog defs can reference an item yaml and things can be bundled together?
            log.warn("Deprecated read of catalog item from sibling keys of `brooklyn.catalog` section, "
                + "instead of the more common appraoch of putting inside an `item` within it. "
                + "Rewrite to use nested/reference syntax instead or contact the community for assistance or feedback.");
            Map<String,?> rootItem = MutableMap.of("item", itemDef);
            String rootItemYaml = yaml;
            YamlExtract yamlExtract = Yamls.getTextOfYamlAtPath(rootItemYaml, "brooklyn.catalog");
            String match = yamlExtract.withOriginalIndentation(true).withKeyIncluded(true).getMatchedYamlTextOrWarn();
            if (match!=null) {
                if (rootItemYaml.startsWith(match)) rootItemYaml = Strings.removeFromStart(rootItemYaml, match);
                else rootItemYaml = Strings.replaceAllNonRegex(rootItemYaml, "\n"+match, "");
            }
            collectCatalogItemsFromItemMetadataBlock("item:\n"+makeAsIndentedObject(rootItemYaml), containingBundle, rootItem, resultLegacyFormat, resultNewFormat, requireValidation, catalogMetadata, 1, force, throwOnError);
        }
    }

    /**
     * Expects item metadata, containing an `item` containing the definition,
     * and/or `items` containing a list of item metadata (recursing with depth).
     * 
     * Supports two modes depending whether <code>result</code> is passed here:
     * 
     * * CatalogItems validated and returned, but not added to catalog here, instead returned in <code>result</code>;
     *   caller does that, and CI instances are persisted and loaded directly after rebind
     *   
     * * RegisteredTypes added to (unpersisted) type registry if <code>result</code> is null;
     *   caller than validates, optionally removes broken ones,
     *   given the ability to add multiple interdependent BOMs/bundles and then validate;
     *   bundles with BOMs are persisted instead of catalog items
     * 
     * I (Alex) think the first one should be considered legacy, and removed once we do
     * everything with bundles. (At that point we can kill nearly ALL the code in this package,
     * needing just a subset of the parsing and validation routines in this class which can
     * be tidied up a lot.)
     * 
     * Parameters suggest other combinations besides the above, but they aren't guaranteed to work.
     * This is a private method and expect to clean it up a lot as per above.
     *  
     * @param sourceYaml - metadata source for reference
     * @param containingBundle - bundle where this is being loaded, or null
     * @param itemMetadata - map of this item metadata reap
     * @param resultLegacyFormat - list where items should be added, or add to type registry if null
     * @param resultNewFormat - map of new->(old or null) for items added
     * @param requireValidation - whether to require items to be validated; if false items might not be valid,
     *     and/or their catalog item types might not be set correctly yet; caller should normally validate later
     *     (useful if we have to load a bunch of things before they can all be validated) 
     * @param parentMetadata - inherited metadata
     * @param depth - depth this is running in
     * @param force - whether to force the catalog addition (does not apply in legacy mode where resultLegacyFormat is non-null)
     */
    @SuppressWarnings("unchecked")
    private void collectCatalogItemsFromItemMetadataBlock(String sourceYaml, ManagedBundle containingBundle, Map<?,?> itemMetadata, List<CatalogItemDtoAbstract<?, ?>> resultLegacyFormat, Map<RegisteredType, RegisteredType> resultNewFormat, boolean requireValidation, 
            Map<?,?> parentMetadata, int depth, boolean force, Boolean throwOnError) {
        if (throwOnError==null) {
            // default for legacy format was to throw, for new format to attempt to add and then remove
            throwOnError = resultLegacyFormat!=null;
        }

        if (sourceYaml==null) sourceYaml = new Yaml().dump(itemMetadata);

        Map<?, ?> itemMetadataWithoutItemDef = MutableMap.builder()
                .putAll(itemMetadata)
                .remove("item")
                .remove("items")
                .build();
        
        // Parse CAMP-YAML DSL in item metadata (but not in item or items - those will be parsed only when used). 
        CampYamlParser parser = mgmt.getScratchpad().get(CampYamlParser.YAML_PARSER_KEY);
        if (parser != null) {
            itemMetadataWithoutItemDef = parser.parse((Map<String, Object>) itemMetadataWithoutItemDef);
            try {
                itemMetadataWithoutItemDef = (Map<String, Object>) Tasks.resolveDeepValueWithoutCoercion(itemMetadataWithoutItemDef, mgmt.getServerExecutionContext());
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
            
        } else {
            log.info("No Camp-YAML parser registered for parsing catalog item DSL; skipping DSL-parsing");
        }

        Map<Object,Object> catalogMetadata = MutableMap.<Object, Object>builder()
                .putAll(parentMetadata)
                .putAll(itemMetadataWithoutItemDef)
                .putIfNotNull("item", itemMetadata.get("item"))
                .putIfNotNull("items", itemMetadata.get("items"))
                .build();
        // tags we treat specially to concatenate as a set (treating as config with merge might be cleaner)
        catalogMetadata.put("tags", MutableSet.copyOf(getFirstAs(parentMetadata, Collection.class, "tags").orNull())
            .putAll(getFirstAs(itemMetadataWithoutItemDef, Collection.class, "tags").orNull()) );

        // brooklyn.libraries we treat specially, to append the list, with the child's list preferred in classloading order
        // `libraries` is supported in some places as a legacy syntax; it should always be `brooklyn.libraries` for new apps
        // TODO in 0.8.0 require brooklyn.libraries, don't allow "libraries" on its own
        List<?> librariesAddedHereNames = MutableList.copyOf(getFirstAs(itemMetadataWithoutItemDef, List.class, "brooklyn.libraries", "libraries").orNull());
        Collection<CatalogBundle> librariesAddedHereBundles = CatalogItemDtoAbstract.parseLibraries(librariesAddedHereNames);
        
        MutableSet<Object> librariesCombinedNames = MutableSet.of();
        if (!isNoBundleOrSimpleWrappingBundle(mgmt, containingBundle)) {
            // ensure containing bundle is declared, first, for search purposes
            librariesCombinedNames.add(containingBundle.getVersionedName().toOsgiString());
        }
        librariesCombinedNames.putAll(librariesAddedHereNames);
        librariesCombinedNames.putAll(getFirstAs(parentMetadata, Collection.class, "brooklyn.libraries", "libraries").orNull());
        if (!librariesCombinedNames.isEmpty()) {
            catalogMetadata.put("brooklyn.libraries", librariesCombinedNames);
        }
        Collection<CatalogBundle> libraryBundles = CatalogItemDtoAbstract.parseLibraries(librariesCombinedNames);

        // TODO this may take a while if downloading; ideally the REST call would be async
        // but this load is required for resolving YAML in this BOM (and if java-scanning);
        // need to think through how we expect dependencies to be installed
        CatalogUtils.installLibraries(mgmt, librariesAddedHereBundles);
        
        // use resolved bundles
        librariesAddedHereBundles = resolveWherePossible(mgmt, librariesAddedHereBundles);
        libraryBundles = resolveWherePossible(mgmt, libraryBundles);

        Boolean scanJavaAnnotations = getFirstAs(itemMetadataWithoutItemDef, Boolean.class, "scanJavaAnnotations", "scan_java_annotations").orNull();
        if (scanJavaAnnotations!=null && scanJavaAnnotations) {
            addLegacyScannedAnnotations(containingBundle, resultLegacyFormat, resultNewFormat, depth, catalogMetadata, librariesAddedHereBundles, libraryBundles);
        }
        
        Object items = catalogMetadata.remove("items");
        Object item = catalogMetadata.remove("item");
        Object url = catalogMetadata.remove("include");

        if (items!=null) {
            int count = 0;
            for (Object ii: checkType(items, "items", List.class)) {
                if (ii instanceof String) {
                    collectUrlReferencedCatalogItems((String) ii, containingBundle, resultLegacyFormat, resultNewFormat, requireValidation, catalogMetadata, depth+1, force, throwOnError);
                } else {
                    Map<?,?> i = checkType(ii, "entry in items list", Map.class);
                    collectCatalogItemsFromItemMetadataBlock(Yamls.getTextOfYamlAtPath(sourceYaml, "items", count).getMatchedYamlTextOrWarn(),
                            containingBundle, i, resultLegacyFormat, resultNewFormat, requireValidation, catalogMetadata, depth+1, force, throwOnError);
                }
                count++;
            }
        }

        if (url != null) {
            collectUrlReferencedCatalogItems(checkType(url, "include in catalog meta", String.class), containingBundle, 
                resultLegacyFormat, resultNewFormat, requireValidation, catalogMetadata, depth+1, force, throwOnError);
        }

        if (item==null) return;

        // now look at the actual item, first correcting the sourceYaml and interpreting the catalog metadata
        String itemYaml = Yamls.getTextOfYamlAtPath(sourceYaml, "item").getMatchedYamlTextOrWarn();
        if (itemYaml!=null) sourceYaml = itemYaml;
        else sourceYaml = new Yaml().dump(item);
        
        CatalogItemType itemType = TypeCoercions.coerce(getFirstAs(catalogMetadata, Object.class, "itemType", "item_type").orNull(), CatalogItemType.class);

        String id = getFirstAs(catalogMetadata, String.class, "id").orNull();
        String version = getFirstAs(catalogMetadata, String.class, "version").orNull();
        String symbolicName = getFirstAs(catalogMetadata, String.class, "symbolicName").orNull();
        String displayName = getFirstAs(catalogMetadata, String.class, "displayName").orNull();
        String name = getFirstAs(catalogMetadata, String.class, "name").orNull();
        String format = getFirstAs(catalogMetadata, String.class, "format").orNull();
        if ("auto".equalsIgnoreCase(format)) format = null;

        if ((Strings.isNonBlank(id) || Strings.isNonBlank(symbolicName)) &&
                Strings.isNonBlank(displayName) &&
                Strings.isNonBlank(name) && !name.equals(displayName)) {
            log.warn("Name property will be ignored due to the existence of displayName and at least one of id, symbolicName");
        }

        CharSequence loggedId = Strings.firstNonBlank(id, symbolicName, displayName, "<unidentified>");
        log.debug("Analyzing item " + loggedId + " for addition to catalog");
        PlanInterpreterInferringType planInterpreter = new PlanInterpreterInferringType(id, item, sourceYaml, itemType, format,
                (containingBundle instanceof CatalogBundle ? ((CatalogBundle)containingBundle) : null), libraryBundles,
                null, resultLegacyFormat).resolve();
        Exception resolutionError = null;
        if (!planInterpreter.isResolved()) {
            // don't throw yet, we may be able to add it in an unresolved state
            resolutionError = Exceptions.create("Could not resolve definition of item"
                + (Strings.isNonBlank(id) ? " '"+id+"'" : Strings.isNonBlank(symbolicName) ? " '"+symbolicName+"'" : Strings.isNonBlank(name) ? " '"+name+"'" : "")
                // better not to show yaml, takes up lots of space, and with multiple plan transformers there might be multiple errors;
                // some of the errors themselves may reproduce it
                // (ideally in future we'll be able to return typed errors with caret position of error)
//                + ":\n"+sourceYaml
                , planInterpreter.getErrors());
        }
        // might be null
        itemType = planInterpreter.getCatalogItemType();

        Map<?, ?> itemAsMap = planInterpreter.getItem();
        // the "plan yaml" includes the services: ... or brooklyn.policies: ... outer key,
        // as opposed to the rawer { type: foo } map without that outer key which is valid as item input

        // if symname not set, infer from: id, then name, then item id, then item name
        if (Strings.isBlank(symbolicName)) {
            if (Strings.isNonBlank(id)) {
                if (RegisteredTypeNaming.isGoodBrooklynTypeColonVersion(id)) {
                    symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(id);
                } else if (RegisteredTypeNaming.isValidOsgiTypeColonVersion(id)) {
                    symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(id);
                    log.warn("Discouraged version syntax in id '"+id+"'; version should comply with brooklyn recommendation (#.#.#-qualifier or portion) or specify symbolic name and version explicitly, not OSGi version syntax");
                } else if (CatalogUtils.looksLikeVersionedId(id)) {
                    // use of above method is deprecated in 0.12; this block can be removed in 0.13
                    log.warn("Discouraged version syntax in id '"+id+"'; version should comply with brooklyn recommendation (#.#.#-qualifier or portion) or specify symbolic name and version explicitly");
                    symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(id);
                } else if (RegisteredTypeNaming.isUsableTypeColonVersion(id)) {
                    log.warn("Deprecated type naming syntax in id '"+id+"'; colons not allowed in type name as it is used to indicate version");
                    // deprecated in 0.12; from 0.13 this can change to treat part after the colon as version, also see line to set version below
                    // (may optionally warn or disallow if we want to require OSGi versions)
                    // symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(id);
                    symbolicName = id;
                } else {
                    symbolicName = id;
                }
            } else if (Strings.isNonBlank(name)) {
                if (RegisteredTypeNaming.isGoodBrooklynTypeColonVersion(name) || RegisteredTypeNaming.isValidOsgiTypeColonVersion(name)) {
                    log.warn("Deprecated use of 'name' key to define '"+name+"'; version should be specified within 'id' key or with 'version' key, not this tag");
                    // deprecated in 0.12; remove in 0.13
                    symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(name);
                } else if (CatalogUtils.looksLikeVersionedId(name)) {
                    log.warn("Deprecated use of 'name' key to define '"+name+"'; version should be specified within 'id' key or with 'version' key, not this tag");
                    // deprecated in 0.12; remove in 0.13
                    symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(name);
                } else if (RegisteredTypeNaming.isUsableTypeColonVersion(name)) {
                    log.warn("Deprecated type naming syntax in id '"+id+"'; colons not allowed in type name as it is used to indicate version");
                    // deprecated in 0.12; throw error if we want in 0.13
                    symbolicName = name;
                } else {
                    symbolicName = name;
                }
            } else {
                symbolicName = setFromItemIfUnset(symbolicName, itemAsMap, "id");
                symbolicName = setFromItemIfUnset(symbolicName, itemAsMap, "name");
                // TODO we should let the plan transformer give us this
                symbolicName = setFromItemIfUnset(symbolicName, itemAsMap, "template_name");
                if (Strings.isBlank(symbolicName)) {
                    log.error("Can't infer catalog item symbolicName from the following plan:\n" + sourceYaml);
                    throw new IllegalStateException("Can't infer catalog item symbolicName from catalog item metadata");
                }
            }
        }

        String versionFromId = null;
        if (RegisteredTypeNaming.isGoodBrooklynTypeColonVersion(id)) {
            versionFromId = CatalogUtils.getVersionFromVersionedId(id);
        } else if (RegisteredTypeNaming.isValidOsgiTypeColonVersion(id)) {
            versionFromId = CatalogUtils.getVersionFromVersionedId(id);
            log.warn("Discouraged version syntax in id '"+id+"'; version should comply with Brooklyn recommended version syntax (#.#.#-qualifier or portion) or specify symbolic name and version explicitly, not OSGi");
        } else if (CatalogUtils.looksLikeVersionedId(id)) {
            log.warn("Discouraged version syntax in id '"+id+"'; version should comply with Brooklyn recommended version syntax (#.#.#-qualifier or portion) or specify symbolic name and version explicitly");
            // remove in 0.13
            versionFromId = CatalogUtils.getVersionFromVersionedId(id);
        } else if (RegisteredTypeNaming.isUsableTypeColonVersion(id)) {
            // deprecated in 0.12, with warning above; from 0.13 this can be uncommented to treat part after the colon as version
            // (may optionally warn or disallow if we want to require OSGi versions)
            // if comparable section above is changed, change this to:
            // versionFromId = CatalogUtils.getVersionFromVersionedId(id);
        }
        
        // if version not set, infer from: id, then from name, then item version
        if (versionFromId!=null) {
            if (Strings.isNonBlank(version) && !versionFromId.equals(version)) {
                throw new IllegalArgumentException("Discrepency between version set in id " + versionFromId + " and version property " + version);
            }
            version = versionFromId;
        }
        
        if (Strings.isBlank(version)) {
            if (CatalogUtils.looksLikeVersionedId(name)) {
                // deprecated in 0.12, remove in 0.13
                log.warn("Deprecated use of 'name' key to define '"+name+"'; version should be specified within 'id' key or with 'version' key, not this tag");
                version = CatalogUtils.getVersionFromVersionedId(name);
            }
            if (Strings.isBlank(version)) {
                version = setFromItemIfUnset(version, itemAsMap, "version");
                version = setFromItemIfUnset(version, itemAsMap, "template_version");
                if (version==null) {
                    log.debug("No version specified for catalog item " + symbolicName + ". Using default value.");
                    version = null;
                }
            }
        }
        
        // if not set, ID can come from symname:version, failing that, from the plan.id, failing that from the sym name
        if (Strings.isBlank(id)) {
            // let ID be inferred, especially from name, to support style where only "name" is specified, with inline version
            if (Strings.isNonBlank(symbolicName) && Strings.isNonBlank(version)) {
                id = symbolicName + ":" + version;
            }
            id = setFromItemIfUnset(id, itemAsMap, "id");
            if (Strings.isBlank(id)) {
                if (Strings.isNonBlank(symbolicName)) {
                    id = symbolicName;
                } else {
                    log.error("Can't infer catalog item id from the following plan:\n" + sourceYaml);
                    throw new IllegalStateException("Can't infer catalog item id from catalog item metadata");
                }
            }
        }

        if (Strings.isBlank(displayName)) {
            if (Strings.isNonBlank(name)) displayName = name;
            displayName = setFromItemIfUnset(displayName, itemAsMap, "name");
        }

        String description = getFirstAs(catalogMetadata, String.class, "description").orNull();
        description = setFromItemIfUnset(description, itemAsMap, "description");

        // icon.url is discouraged (using '.'), but kept for legacy compatibility; should deprecate this
        String catalogIconUrl = getFirstAs(catalogMetadata, String.class, "iconUrl", "icon_url", "icon.url").orNull();
        catalogIconUrl = setFromItemIfUnset(catalogIconUrl, itemAsMap, "iconUrl", "icon_url", "icon.url");

        final String deprecated = getFirstAs(catalogMetadata, String.class, "deprecated").orNull();
        final Boolean catalogDeprecated = Boolean.valueOf(setFromItemIfUnset(deprecated, itemAsMap, "deprecated"));

        // run again now that we know the ID to catch recursive definitions and possibly other mistakes (itemType inconsistency?)
        planInterpreter = planInterpreter.setId(id).resolve();
        if (resolutionError==null && !planInterpreter.isResolved()) {
            resolutionError = new IllegalStateException("Plan resolution for "+id+" breaks after id and itemType are set; is there a recursive reference or other type inconsistency?\n"+sourceYaml);
        }
        if (throwOnError && resolutionError!=null) {
            // if there was an error, throw it here
            throw Exceptions.propagate(resolutionError);
        }

        String sourcePlanYaml = planInterpreter.getPlanYaml();

        if (resultLegacyFormat==null) {
            // horrible API but basically `resultLegacyFormat==null` means use the new-style,
            // adding from persisted bundles to type registry (which is not persisted)
            // instead of old way which persisted catalog items (and not their bundles).
            // this lets us deal with forward references, with a subsequent step to validate.

            Set<Object> tags = MutableSet.of().putAll(getFirstAs(catalogMetadata, Collection.class, "tags").orNull());
            
            List<String> aliases = MutableList.of();
            // could easily allow aliases to be set in catalog.bom, as done for tags above,
            // but currently we don't, we only allow the official type name
            
            Boolean catalogDisabled = null;
            
            MutableList<Object> superTypes = MutableList.of();

            if (itemType==CatalogItemType.TEMPLATE) {
                tags.add(BrooklynTags.CATALOG_TEMPLATE);
                itemType = CatalogItemType.APPLICATION;
            }
            if (itemType==CatalogItemType.APPLICATION) {
                itemType = CatalogItemType.ENTITY;
                superTypes.add(Application.class);
            }
            
            if (resolutionError!=null) {
                if (!tags.contains(BrooklynTags.CATALOG_TEMPLATE)) {
                    if (requireValidation) {
                        throw Exceptions.propagate(resolutionError);
                    }
                    // warn? add as "unresolved" ? just do nothing?
                }
            }

            if (itemType!=null) {
                // if supertype is known, set it here;
                // we don't set kind (spec) because that is inferred from the supertype type
                superTypes.appendIfNotNull(BrooklynObjectType.of(itemType).getInterfaceType());
            }
            
            if (version==null) {
                if (containingBundle!=null) {
                    version = containingBundle.getVersionedName().getVersionString();
                }
                if (version==null) {
                    // use this as default version when nothing specified or inferrable from containing bundle
                    version = BasicBrooklynCatalog.NO_VERSION;
                }
            }
            
            if (sourcePlanYaml==null) {
                // happens if unresolved and not valid yaml, replace with item yaml
                // which normally has "type: " prefixed
                sourcePlanYaml = planInterpreter.itemYaml;
            }
            
            BasicTypeImplementationPlan plan = new BasicTypeImplementationPlan(format, sourcePlanYaml);
            BasicRegisteredType type = (BasicRegisteredType) RegisteredTypes.newInstance(
                    BrooklynObjectType.of(planInterpreter.catalogItemType).getSpecType()!=null ? RegisteredTypeKind.SPEC
                            : planInterpreter.catalogItemType==CatalogItemType.BEAN ? RegisteredTypeKind.BEAN
                            : RegisteredTypeKind.UNRESOLVED,
                symbolicName, version, plan,
                superTypes, aliases, tags, containingBundle==null ? null : containingBundle.getVersionedName().toString(), 
                MutableList.<OsgiBundleWithUrl>copyOf(libraryBundles), 
                displayName, description, catalogIconUrl, catalogDeprecated, catalogDisabled);
            RegisteredTypes.notePlanEquivalentToThis(type, plan);
            // record original source in case it was changed
            RegisteredTypes.notePlanEquivalentToThis(type, new BasicTypeImplementationPlan(format, sourceYaml));
            
            RegisteredType replacedInstance = mgmt.getTypeRegistry().get(type.getSymbolicName(), type.getVersion());

            log.debug("Analyzed " + loggedId + " as " + type + " (" + Strings.firstNonNull(planInterpreter.catalogItemType, "unresolved") + "), adding to type registry " +
                    (planInterpreter.catalogItemType==null ? "(despite errors at this stage, "+planInterpreter.getErrors().stream().findFirst().orElse(null)+"; may be resolved later once other items are added, or may fail later)"
                        : "(will re-resolve as registered type later)"));

            ((BasicBrooklynTypeRegistry) mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(type, force);
            updateResultNewFormat(resultNewFormat, replacedInstance, type);
            
        } else {
            CatalogItemDtoAbstract<?, ?> dto = createItemBuilder(itemType, symbolicName, version)
                .libraries(libraryBundles)
                .displayName(displayName)
                .description(description)
                .deprecated(catalogDeprecated)
                .iconUrl(catalogIconUrl)
                .plan(sourcePlanYaml)
                .build();
    
            dto.setManagementContext((ManagementContextInternal) mgmt);
            log.debug("Analyzed " + loggedId + " as " + dto + " (" + planInterpreter.catalogItemType + "), adding to legacy catalog");

            resultLegacyFormat.add(dto);
        }
    }

    private void addLegacyScannedAnnotations(ManagedBundle containingBundle, List<CatalogItemDtoAbstract<?, ?>> resultLegacyFormat, Map<RegisteredType, RegisteredType> resultNewFormat, int depth, Map<Object, Object> catalogMetadata, Collection<CatalogBundle> librariesAddedHereBundles, Collection<CatalogBundle> libraryBundles) {
        log.warn("Deprecated use of scanJavaAnnotations" + (containingBundle != null ? " in bundle " + containingBundle.getVersionedName() : ""));

        if (isNoBundleOrSimpleWrappingBundle(mgmt, containingBundle)) {
            Collection<CatalogItemDtoAbstract<?, ?>> scanResult;
            // BOMs wrapped in JARs, or without JARs, have special treatment
            if (isLibrariesMoreThanJustContainingBundle(librariesAddedHereBundles, containingBundle)) {
                // legacy mode, since 0.12.0, scan libraries referenced in a legacy non-bundle BOM
                log.warn("Deprecated use of scanJavaAnnotations to scan other libraries ("+ librariesAddedHereBundles +"); libraries should declare they scan themselves");
                scanResult = scanAnnotationsLegacyInListOfLibraries(mgmt, librariesAddedHereBundles, catalogMetadata, containingBundle);
            } else if (!isLibrariesMoreThanJustContainingBundle(libraryBundles, containingBundle)) {
                // for default catalog, no libraries declared, we want to scan local classpath
                // bundle should be named "brooklyn-default-catalog"
                if (containingBundle !=null && !containingBundle.getSymbolicName().contains("brooklyn-default-catalog")) {
                    // a user uplaoded a BOM trying to tell us to do a local java scan; previously supported but becoming unsupported
                    log.warn("Deprecated use of scanJavaAnnotations in non-Java BOM outwith the default catalog setup");
                } else if (depth >0) {
                    // since 0.12.0, require this to be right next to where libraries are defined, or at root
                    log.warn("Deprecated use of scanJavaAnnotations declared in item; should be declared at the top level of the BOM");
                }
                scanResult = scanAnnotationsFromLocalNonBundleClasspath(mgmt, catalogMetadata, containingBundle);
            } else {
                throw new IllegalStateException("Cannot scan for Java catalog items when libraries declared on an ancestor; scanJavaAnnotations should be specified alongside brooklyn.libraries (or ideally those libraries should specify to scan)");
            }
            if (scanResult!=null && !scanResult.isEmpty()) {
                if (resultLegacyFormat !=null) {
                    resultLegacyFormat.addAll( scanResult );
                } else {
                    // not returning a result; we need to add here, as type
                    for (CatalogItem item: scanResult) {
                        RegisteredType replacedInstance = mgmt.getTypeRegistry().get(item.getSymbolicName(), item.getVersion());
                        mgmt.getCatalog().addItem(item);
                        RegisteredType newInstance = mgmt.getTypeRegistry().get(item.getSymbolicName(), item.getVersion());
                        updateResultNewFormat(resultNewFormat, replacedInstance, newInstance);
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Scanning for Java annotations is not supported in BOMs in bundles; "
                + "entries should be listed explicitly in the catalog.bom");
        }
    }

    private void updateResultNewFormat(Map<RegisteredType, RegisteredType> resultNewFormat, RegisteredType replacedInstance,
        RegisteredType newInstance) {
        if (resultNewFormat!=null) {
            if (resultNewFormat.containsKey(newInstance)) {
                log.debug("Multiple definitions for "+newInstance+" in BOM; only recording one");
            } else {
                resultNewFormat.put(newInstance, replacedInstance);
            }
        }
    }

    protected static Collection<CatalogBundle> resolveWherePossible(ManagementContext mgmt, Collection<CatalogBundle> libraryBundles) {
        Collection<CatalogBundle> libraryBundlesResolved = MutableSet.of();
        for (CatalogBundle b: libraryBundles) {
            libraryBundlesResolved.add(CatalogBundleDto.resolve(mgmt, b).or(b));
        }
        return libraryBundlesResolved;
    }

    private static boolean isLibrariesMoreThanJustContainingBundle(Collection<CatalogBundle> library, ManagedBundle containingBundle) {
        if (library==null || library.isEmpty()) return false;
        if (containingBundle==null) return !library.isEmpty();
        if (library.size()>1) return true;
        CatalogBundle li = Iterables.getOnlyElement(library);
        return !containingBundle.getVersionedName().equalsOsgi(li.getVersionedName());
    }

    @Beta
    public static boolean isNoBundleOrSimpleWrappingBundle(ManagementContext mgmt, ManagedBundle b) {
        if (b==null) return true;
        Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
        if (osgi.isAbsent()) {
            // odd, shouldn't happen, installing bundle but not using osgi
            throw new IllegalStateException("OSGi not being used but installing a bundle");
        }
        Maybe<Bundle> bb = osgi.get().findBundle(b);
        if (bb.isAbsent()) {
            // odd, shouldn't happen, bundle not managed
            // (originally seen during a race where the empty-remover ran while we were installing)
            throw new IllegalStateException("Loading from a bundle which is not installed");
        }
        return isWrapperBundle(bb.get());
    }
    
    @Beta
    public static boolean isWrapperBundle(Bundle b) {
        String wrapped = b.getHeaders().get(BROOKLYN_WRAPPED_BOM_BUNDLE);
        return wrapped!=null && wrapped.equalsIgnoreCase("true");
    }

    private void collectUrlReferencedCatalogItems(String url, ManagedBundle containingBundle, List<CatalogItemDtoAbstract<?, ?>> resultLegacyFormat, Map<RegisteredType, RegisteredType> resultNewFormat, boolean requireValidation, Map<Object, Object> parentMeta, int depth, boolean force, Boolean throwOnError) {
        @SuppressWarnings("unchecked")
        List<?> parentLibrariesRaw = MutableList.copyOf(getFirstAs(parentMeta, Iterable.class, "brooklyn.libraries", "libraries").orNull());
        Collection<CatalogBundle> parentLibraries = CatalogItemDtoAbstract.parseLibraries(parentLibrariesRaw);
        BrooklynClassLoadingContext loader = CatalogUtils.newClassLoadingContext(mgmt, "<catalog url reference loader>:0.0.0", parentLibraries);
        String yaml;
        log.debug("Catalog load, loading referenced BOM at "+url+" as part of "+(containingBundle==null ? "non-bundled load" : containingBundle.getVersionedName())+" ("+(resultNewFormat!=null ? resultNewFormat.size() : resultLegacyFormat!=null ? resultLegacyFormat.size() : "(unknown)")+" items before load)");
        if (url.startsWith("http")) {
            // give greater visibility to these
            log.info("Loading external referenced BOM at "+url+" as part of "+(containingBundle==null ? "non-bundled load" : containingBundle.getVersionedName()));
        }
        try {
            yaml = ResourceUtils.create(loader).getResourceAsString(url);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new IllegalStateException("Remote catalog url " + url + " in "+(containingBundle==null ? "non-bundled load" : containingBundle.getVersionedName())+" can't be fetched.", e);
        }
        try {
            collectCatalogItemsFromCatalogBomRoot("BOM expected at "+url, yaml, containingBundle, resultLegacyFormat, resultNewFormat, requireValidation, parentMeta, depth, force, throwOnError);
        } catch (Exception e) {
            Exceptions.propagateAnnotated("Error loading "+url+" as part of "+(containingBundle==null ? "non-bundled load" : containingBundle.getVersionedName()), e);
        }
        log.debug("Catalog load, loaded referenced BOM at "+url+" as part of "+(containingBundle==null ? "non-bundled load" : containingBundle.getVersionedName())+", now have "+
            (resultNewFormat!=null ? resultNewFormat.size() : resultLegacyFormat!=null ? resultLegacyFormat.size() : "(unknown)")+" items");
    }

    @SuppressWarnings("unchecked")
    private <T> T checkType(Object x, String description, Class<T> type) {
        if (type.isInstance(x)) return (T)x;
        throw new UserFacingException("Expected "+JavaClassNames.superSimpleClassName(type)+" for "+description+", not "+JavaClassNames.superSimpleClassName(x));
    }

    private String setFromItemIfUnset(String oldValue, Map<?,?> item, String ...fieldAttrs) {
        if (Strings.isNonBlank(oldValue)) return oldValue;
        if (item!=null) {
            for (String fieldAttr: fieldAttrs) {
                Object newValue = item.get(fieldAttr);
                if (newValue instanceof String && Strings.isNonBlank((String)newValue)) {
                    return (String)newValue;
                } else if (newValue instanceof Number || newValue instanceof Boolean) {
                    return newValue.toString();
                }
            }
        }
        return oldValue;
    }

    /**
     * @deprecated since 1.0.0; Catalog annotation is deprecated.
     */
    @Deprecated
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsFromLocalNonBundleClasspath(ManagementContext mgmt, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Local Scanned Catalog", "All annotated Brooklyn entities detected in the classpath", "scanning-local-classpath");
        return scanAnnotationsInternal(mgmt, new CatalogDo(dto), catalogMetadata, containingBundle);
    }
    
    /**
     * @deprecated since 1.0.0; Catalog annotation is deprecated.
     */
    @Deprecated
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsLegacyInListOfLibraries(ManagementContext mgmt, Collection<? extends OsgiBundleWithUrl> libraries, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Bundles Scanned Catalog", "All annotated Brooklyn entities detected in bundles", "scanning-bundles-classpath-"+libraries.hashCode());
        List<String> urls = MutableList.of();
        for (OsgiBundleWithUrl b: libraries) {
            // does not support pre-installed bundles identified by name:version 
            // (ie where URL not supplied)
            if (Strings.isNonBlank(b.getUrl())) {
                urls.add(b.getUrl());
            } else {
                log.warn("scanJavaAnnotations does not apply to pre-installed bundles; skipping "+b);
            }
        }
        
        if (urls.isEmpty()) {
            log.warn("No bundles to scan: scanJavaAnnotations currently only applies to OSGi bundles provided by URL"); 
            return MutableList.of();
        }
        
        CatalogDo subCatalog = new CatalogDo(dto);
        subCatalog.addToClasspath(urls.toArray(new String[0]));
        return scanAnnotationsInternal(mgmt, subCatalog, catalogMetadata, containingBundle);
    }
    
    /**
     * @deprecated since 1.0.0; Catalog annotation is deprecated.
     */
    @Deprecated
    @SuppressWarnings("unused")  // keep during 0.12.0 until we are decided we won't support this; search for this method name
    // (note that this now could work after rebind since we have the OSGi cache)
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsInBundle(ManagementContext mgmt, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Bundle "+containingBundle.getVersionedName().toOsgiString()+" Scanned Catalog", "All annotated Brooklyn entities detected in bundles", "scanning-bundle-"+containingBundle.getVersionedName().toOsgiString());
        CatalogDo subCatalog = new CatalogDo(dto);
        // need access to a JAR to scan this
        String url = null;
        File f = ((ManagementContextInternal)mgmt).getOsgiManager().get().getBundleFile(containingBundle);
        if (f!=null) {
            url = "file//:"+f.getAbsolutePath();
        }
        if (url==null) {
            url = containingBundle.getUrl();
        }
        if (url==null) {
            // should now always be available 
            throw new IllegalArgumentException("Error preparing to scan "+containingBundle.getVersionedName()+": no URL available");
        }
        // org.reflections requires the URL to be "file:" containg ".jar"
        File fJar = Os.newTempFile(containingBundle.getVersionedName().toOsgiString(), ".jar");
        try {
            Streams.copy(ResourceUtils.create().getResourceFromUrl(url), new FileOutputStream(fJar));
            subCatalog.addToClasspath(new String[] { "file:"+fJar.getAbsolutePath() });
            Collection<CatalogItemDtoAbstract<?, ?>> result = scanAnnotationsInternal(mgmt, subCatalog, MutableMap.of("version", containingBundle.getSuppliedVersionString()), containingBundle);
            return result;
        } catch (FileNotFoundException e) {
            throw Exceptions.propagateAnnotated("Error extracting "+url+" to scan "+containingBundle.getVersionedName(), e);
        } finally {
            fJar.delete();
        }
    }

    /**
     * @deprecated since 1.0.0; Catalog annotation is deprecated.
     */
    @Deprecated
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsInternal(ManagementContext mgmt, CatalogDo subCatalog, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        subCatalog.mgmt = mgmt;
        subCatalog.setClasspathScanForEntities(CatalogScanningModes.ANNOTATIONS);
        subCatalog.load();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Collection<CatalogItemDtoAbstract<?, ?>> result = (Collection)Collections2.transform(
                (Collection<CatalogItemDo<Object,Object>>)(Collection)subCatalog.getIdCache().values(), 
                itemDoToDtoAddingSelectedMetadataDuringScan(mgmt, catalogMetadata, containingBundle));
        return result;
    }

    private class PlanInterpreterInferringType {

        String itemId;
        @Nonnull
        final Map<?,?> item;
        final String itemYaml;
        final String format;
        final CatalogBundle containingBundle;
        final Collection<CatalogBundle> libraryBundles;
        final List<CatalogItemDtoAbstract<?, ?>> itemsDefinedSoFar;
        RegisteredTypeLoadingContext constraint;

        CatalogItemType catalogItemType;
        String planYaml;
        boolean resolved = false;
        List<Exception> errors = MutableList.of();
        List<Exception> entityErrors = MutableList.of();
        List<Exception> transformerErrors = MutableList.of();

        public PlanInterpreterInferringType(@Nullable String itemId, Object itemDefinitionParsedToStringOrMap, String itemYaml, @Nullable CatalogItemType optionalCiType, @Nullable String format,
                                            CatalogBundle containingBundle, Collection<CatalogBundle> libraryBundles,
                                            RegisteredTypeLoadingContext constraint, List<CatalogItemDtoAbstract<?,?>> itemsDefinedSoFar) {
            // ID is useful to prevent recursive references (possibly only supported for entities?)
            this.itemId = itemId;
            this.containingBundle = containingBundle;

            this.constraint = constraint;

            if (itemDefinitionParsedToStringOrMap instanceof String) {
                if (((String)itemDefinitionParsedToStringOrMap).trim().indexOf("\n")<0) {
                    // if just a one-line string supplied, treat at type unless it parses as a map
                    Object reparsed = null;
                    try {
                        reparsed = Iterables.getOnlyElement( Yamls.parseAll( (String) itemDefinitionParsedToStringOrMap ) );
                    } catch (Exception e) {
                        // unparseable, leave null to treat as the type
                    }
                    if (reparsed instanceof Map) {
                        this.item = (Map<?,?>) reparsed;
                        this.itemYaml = (String) itemDefinitionParsedToStringOrMap;
                    } else {
                        this.item = MutableMap.of("type", itemDefinitionParsedToStringOrMap);
                        this.itemYaml = "type: "+itemYaml;
                    }
                } else {
                    // if multi-line, treat as template, not parsed
                    this.item = MutableMap.of();
                    this.itemYaml = (String) itemDefinitionParsedToStringOrMap;                
                }
            } else if (itemDefinitionParsedToStringOrMap instanceof Map) {
                this.item = (Map<?,?>)itemDefinitionParsedToStringOrMap;
                this.itemYaml = itemYaml;
            } else {
                throw new IllegalArgumentException("Item definition should be a string or map to use the guesser");
            }
            this.catalogItemType = optionalCiType;
            this.format = format;
            this.libraryBundles = libraryBundles;
            this.itemsDefinedSoFar = itemsDefinedSoFar;
        }

        public PlanInterpreterInferringType resolve() {
            try {
                currentlyResolvingType.set(Strings.isBlank(itemId) ? itemYaml : itemId);

                Maybe<Object> transformedResult = attemptPlanTranformer();
                boolean onlyNewStyleTransformer = format != null || catalogItemType == CatalogItemType.BEAN;
                if (transformedResult.isPresent() || onlyNewStyleTransformer) {
                    planYaml = itemYaml;
                    resolved = transformedResult.isPresent() || catalogItemType == CatalogItemType.TEMPLATE;
                    if (!resolved) {
                        errors.add(Maybe.Absent.getException(transformedResult));
                    }
                    if (resolved && catalogItemType != CatalogItemType.BEAN && catalogItemType != CatalogItemType.TEMPLATE &&
                            (format==null || !"brooklyn-camp".equals(format))) {
                        // for spec types, _also_ run the legacy resolution because it is better at spotting some types of errors (recursive ones);
                        // note this code will also run if there was an error when format was specified (other than bean-with-type) and we couldn't determine it was a bean
                        resolved = false;
                        attemptLegacySpecTransformersForVariousSpecTypes();
                    }
                    return this;
                }

                // for now, these are the lowest-priority errors (reported after the others)
                transformerErrors.add(((Maybe.Absent) transformedResult).getException());

                if (catalogItemType == CatalogItemType.TEMPLATE) {
                    // template *must* be explicitly specified as item type, and if so, the "various" methods below don't apply,
                    // and we always mark it as resolved.  (probably not necessary to do any of the transformers!)
                    attemptLegacySpecTransformersForType(null, CatalogItemType.TEMPLATE);
                    if (!resolved) {
                        // anything goes, for an explicit template, because we can't easily recurse into the types
                        planYaml = itemYaml;
                        resolved = true;
                    }
                    return this;
                }

                // couldn't resolve it with the plan transformers; retry with legacy "spec" transformers.
                // TODO this legacy path is still needed where an entity is declared with nice abbreviated 'type: xxx' syntax, not the full-camp 'services: [ { type: xxx } ]' syntax.
                // would be nice to move that logic internally to CAMP and see if we can remove this altogether.
                // (see org.apache.brooklyn.camp.brooklyn.spi.creation.CampResolver.createEntitySpecFromServicesBlock )
                if (format == null) {
                    attemptLegacySpecTransformersForVariousSpecTypes();
                }

                return this;
            } finally {
                currentlyResolvingType.remove();
            }
        }

        private void attemptLegacySpecTransformersForVariousSpecTypes() {
            attemptLegacySpecTransformersForType(null, CatalogItemType.ENTITY);

            List<Exception> oldEntityErrors = MutableList.copyOf(entityErrors);
            // try with services key
            attemptLegacySpecTransformersForType("services", CatalogItemType.ENTITY);
            entityErrors.removeAll(oldEntityErrors);
            entityErrors.addAll(oldEntityErrors);
            // errors when wrapped in services block are better currently
            // as we parse using CAMP and need that
            // so prefer those for now (may change with YOML)

            attemptLegacySpecTransformersForType(POLICIES_KEY, CatalogItemType.POLICY);
            attemptLegacySpecTransformersForType(ENRICHERS_KEY, CatalogItemType.ENRICHER);
            attemptLegacySpecTransformersForType(LOCATIONS_KEY, CatalogItemType.LOCATION);
        }

        boolean suspicionOfABean = false;

        private Maybe<Object> attemptPlanTranformer() {
            MutableSet<Throwable> exceptions = MutableSet.<Throwable>of();
            try {
                suspicionOfABean = false;

                Set<? extends OsgiBundleWithUrl> searchBundles = MutableSet.copyOf(libraryBundles)
                        .putIfNotNull(containingBundle);
                BrooklynClassLoadingContext loader = new OsgiBrooklynClassLoadingContext(mgmt, null, searchBundles);
                if (catalogItemType == null) {
                    // attempt to detect whether it is a bean
                    Object type = item.get("type");
                    if (type!=null && type instanceof String) {
                        TypeToken<?> clz = new BrooklynTypeNameResolver((String)type, loader, false, true)
                                .findTypeToken((String) type).orNull();
                        if (clz!=null) {
                            if (!BrooklynObject.class.isAssignableFrom(TypeTokens.getRawRawType(clz))) {
                                suspicionOfABean = true;
                            }
                        }
                    }
                }

                if (constraint==null) {
                    constraint = RegisteredTypeLoadingContexts.loaderAlreadyEncountered(loader, null, itemId);
                } else {
                    constraint = RegisteredTypeLoadingContexts.withLoader(constraint, loader);
                    constraint = RegisteredTypeLoadingContexts.withEncounteredItem(constraint, itemId);
                }

                Object t = null;
                boolean triedBean = false;
                // try as bean first if signs are auspicious
                if (catalogItemType == CatalogItemType.BEAN || suspicionOfABean) {
                    try {
                        triedBean = true;
                        t = mgmt.getTypeRegistry().createBeanFromPlan(format, itemYaml, constraint, null);
                        catalogItemType = CatalogItemType.BEAN;
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        exceptions.add(e);
                    }
                }

                // then try as spec unless known to be a bean
                if (catalogItemType != CatalogItemType.BEAN && t==null) {
                    try {
                        t = mgmt.getTypeRegistry().createSpecFromPlan(format, itemYaml, constraint,
                                BrooklynObjectType.of(catalogItemType).getSpecType());
                        if (catalogItemType == null) {
                            catalogItemType = CatalogItemType.ofSpecClass(BrooklynObjectType.of(t.getClass()).getSpecType());
                        }
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        exceptions.add(e);
                    }
                }

                // lastly try as bean if we haven't already and it is not known to be a spec (ie if item type is unknown)
                if (catalogItemType==null && t==null && !triedBean) {
                    try {
                        triedBean = true;
                        t = mgmt.getTypeRegistry().createBeanFromPlan(format, itemYaml, constraint, null);
                        if (format==null && (t instanceof Map || t instanceof Collection)) {
                            // doesn't look like a bean
                        } else {
                            catalogItemType = CatalogItemType.BEAN;
                        }
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        if (!exceptions.isEmpty()
                                && Exceptions.getFirstThrowableOfType(exceptions.iterator().next(), UnsupportedTypePlanException.class)!=null
                                && Exceptions.getFirstThrowableOfType(e, UnsupportedTypePlanException.class)==null) {
                            // put it first if spec is unsupported but bean is supported
                            MutableSet<Throwable> e2 = MutableSet.<Throwable>of(e).putAll(exceptions);
                            exceptions.clear();
                            exceptions.addAll(e2);
                        } else {
                            exceptions.add(e);
                        }
                    }
                }

                if (t!=null) {
                    resolved = true;
                    return Maybe.of(t);
                }


            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                exceptions.add(e);
            }

            if (exceptions.isEmpty()) exceptions.add(new IllegalStateException("Type registry creation returned null"));

            return Maybe.absent(
                    () ->
                        Exceptions.create("Unable to transform definition of "+
                            (itemId!=null ? itemId : "plan:\n"+itemYaml+"\n"),
                            exceptions)
            );
        }

        public boolean isResolved() { return resolved; }
        
        /** Returns potentially useful errors encountered while guessing types. 
         * May only be available where the type is known. */
        public List<Exception> getErrors() {
            // errors are useful in this order, at least historically, and in our tests
            MutableList<Exception> l = MutableList.copyOf(errors);
            if (suspicionOfABean && !transformerErrors.isEmpty()) {
                // suppress entity errors
            } else {
                l.appendAll(entityErrors);
            }
            l.appendAll(transformerErrors);
            return l;
        }
        
        public CatalogItemType getCatalogItemType() {
            return catalogItemType; 
        }
        
        public String getPlanYaml() {
            return planYaml;
        }
        
        private boolean attemptLegacySpecTransformersForType(String key, CatalogItemType candidateCiType) {
            if (resolved) return false;
            if (catalogItemType!=null && catalogItemType!=candidateCiType) return false;

            final String candidateYaml;
            if (key==null) candidateYaml = itemYaml;
            else {
                if (item.containsKey(key))
                    candidateYaml = itemYaml;
                else
                    candidateYaml = key + ":\n" + makeAsIndentedList(itemYaml);
            }
            String type = (String) item.get("type");
            if (itemsDefinedSoFar!=null) {
                // first look in collected items, if a key is given
                
                if (type!=null && key!=null) {
                    for (CatalogItemDtoAbstract<?,?> candidate: itemsDefinedSoFar) {
                        if (candidateCiType == candidate.getCatalogItemType() &&
                                (type.equals(candidate.getSymbolicName()) || type.equals(candidate.getId()))) {
                            // matched - exit
                            catalogItemType = candidateCiType;
                            planYaml = candidateYaml;
                            resolved = true;
                            return true;
                        }
                    }
                }
            }
            
            // then try parsing plan - this will use loader
            try {
                @SuppressWarnings("rawtypes")
                CatalogItem itemToAttempt = createItemBuilder(candidateCiType, getIdWithRandomDefault(), DEFAULT_VERSION)
                    .plan(candidateYaml)
                    .libraries(libraryBundles)
                    .build();
                @SuppressWarnings("unchecked")
                AbstractBrooklynObjectSpec<?, ?> spec = internalCreateSpecLegacy(mgmt, itemToAttempt, MutableSet.<String>of(), true);
                if (spec!=null) {
                    catalogItemType = candidateCiType;
                    planYaml = candidateYaml;
                    resolved = true;
                }
                return true;
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                // record the error if we have reason to expect this guess to succeed
                if (item.containsKey("services") && (candidateCiType==CatalogItemType.ENTITY || candidateCiType==CatalogItemType.APPLICATION || candidateCiType==CatalogItemType.TEMPLATE)) {
                    // explicit services supplied, so plan should have been parseable for an entity or a a service
                    errors.add(e);
                } else if (catalogItemType!=null && key!=null) {
                    // explicit itemType supplied, so plan should be parseable in the cases where we're given a key
                    // (when we're not given a key, the previous block should apply)
                    errors.add(e);
                } else {
                    // all other cases, the error is probably due to us not getting the type right, probably ignore it
                    // but cache it if we've checked entity, we'll use that as fallback errors
                    if (candidateCiType==CatalogItemType.ENTITY) {
                        entityErrors.add(e);
                    }
                    if (log.isTraceEnabled()) 
                        log.trace("Guessing type of plan, it looks like it isn't "+candidateCiType+"/"+key+": "+e);
                }
            }
            
            // finally try parsing a cut-down plan, in case there is a nested reference to a newly defined catalog item
            if (type!=null && key!=null) {
                try {
                    String cutDownYaml = key + ":\n" + makeAsIndentedList("type: "+type);
                    @SuppressWarnings("rawtypes")
                    CatalogItem itemToAttempt = createItemBuilder(candidateCiType, getIdWithRandomDefault(), DEFAULT_VERSION)
                            .plan(cutDownYaml)
                            .libraries(libraryBundles)
                            .build();
                    @SuppressWarnings("unchecked")
                    AbstractBrooklynObjectSpec<?, ?> cutdownSpec = internalCreateSpecLegacy(mgmt, itemToAttempt, MutableSet.<String>of(), true);
                    if (cutdownSpec!=null) {
                        catalogItemType = candidateCiType;
                        planYaml = candidateYaml;
                        resolved = true;
                    }
                    return true;
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                }
            }
            // FIXME we should lookup type in the catalog on its own, then infer the type from that,
            // and give proper errors (right now e.g. if there are no transformers then we bail out 
            // with very little information)
            
            return false;
        }

        private String getIdWithRandomDefault() {
            return itemId != null ? itemId : Strings.makeRandomId(10);
        }
        public Map<?,?> getItem() {
            return item;
        }

        public PlanInterpreterInferringType setId(String id) {
            this.itemId = itemId;
            return this;
        }
    }

    /** records the type this catalog is currently trying to resolve items being added to the catalog, if it is trying to resolve.
     * primarily used to downgrade log messages when trying to resolve with different strategies.
     * can also be used to say which item is being currently resolved.
     */
    public static ThreadLocal<String> currentlyResolvingType = new ThreadLocal<String>();

    private String makeAsIndentedList(String yaml) {
        String[] lines = yaml.split("\n");
        lines[0] = "- "+lines[0];
        for (int i=1; i<lines.length; i++)
            lines[i] = "  " + lines[i];
        return Strings.join(lines, "\n");
    }

    private String makeAsIndentedObject(String yaml) {
        String[] lines = yaml.split("\n");
        for (int i=0; i<lines.length; i++)
            lines[i] = "  " + lines[i];
        return Strings.join(lines, "\n");
    }

    static CatalogItemBuilder<?> createItemBuilder(CatalogItemType itemType, String symbolicName, String version) {
        return CatalogItemBuilder.newItem(itemType, symbolicName, version);
    }

    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml) {
        return addItems(yaml, true, false);
    }
    
    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml, boolean validate, boolean forceUpdate) {
        Maybe<OsgiManager> osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager();
        if (osgiManager.isPresent() && AUTO_WRAP_CATALOG_YAML_AS_BUNDLE) {
            // wrap in a bundle to be managed; need to get bundle and version from yaml
            OsgiBundleInstallationResult result = addItemsOsgi(yaml, forceUpdate, osgiManager.get());
            // above will have done validation and supertypes recorded
            return toLegacyCatalogItems(result.getTypesInstalled());

            // if all items pertaining to an older anonymous catalog.bom bundle have been overridden
            // we delete those later; see list of wrapper bundles kept in OsgiManager
        }
        // fallback to non-OSGi for tests and other environments
        return addItems(yaml, null, forceUpdate);
    }
    
    /** Wraps the given items in an OSGi bundle and adds the bundle.
     * If OSGi not present, uses {@link #addItems(String, boolean, boolean)}. */
    @SuppressWarnings("deprecation")
    public OsgiBundleInstallationResult addItemsBundleResult(String yaml, boolean forceUpdate) {
        Maybe<OsgiManager> osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager();
        if (osgiManager.isPresent() && AUTO_WRAP_CATALOG_YAML_AS_BUNDLE) {
            // wrap in a bundle to be managed; need to get bundle and version from yaml
            return addItemsOsgi(yaml, forceUpdate, osgiManager.get());

            // if all items pertaining to an older anonymous catalog.bom bundle have been overridden
            // we delete those later; see list of wrapper bundles kept in OsgiManager
        }
        // fallback to non-OSGi for tests and other environments
        List<? extends CatalogItem<?, ?>> items = addItems(yaml, null, forceUpdate);
        OsgiBundleInstallationResult result = new OsgiBundleInstallationResult();
        for (CatalogItem<?, ?> ci: items) {
            RegisteredType rt = mgmt.getTypeRegistry().get(ci.getId());
            result.getTypesInstalled().add(rt!=null ? rt : RegisteredTypes.of(ci));
        }
        return result;
    }

    protected OsgiBundleInstallationResult addItemsOsgi(String yaml, boolean forceUpdate, OsgiManager osgiManager) {
        return osgiManager.install(InputStreamSource.of("addItemsOsgi supplied yaml", yaml.getBytes()), BrooklynBomYamlCatalogBundleResolver.FORMAT, forceUpdate).get();
    }
    
    @SuppressWarnings("deprecation")
    private List<CatalogItem<?,?>> toLegacyCatalogItems(Iterable<RegisteredType> list) {
        List<CatalogItem<?,?>> result = MutableList.of();
        for (RegisteredType t: list) {
            String id = t.getId();
            CatalogItem<?, ?> item = CatalogUtils.getCatalogItemOptionalVersion(mgmt, id);
            if (item==null) {
                // using new Type Registry (OSGi addition); 
                result.add(RegisteredTypes.toPartialCatalogItem( mgmt.getTypeRegistry().get(id) ));
            } else {
                result.add(item);
            }
        }
        return result;
    }
    
    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml, ManagedBundle bundle, boolean forceUpdate) {
        log.debug("Adding catalog item to "+mgmt+": "+yaml);
        checkNotNull(yaml, "yaml");
        List<CatalogItemDtoAbstract<?, ?>> result = MutableList.of();
        collectCatalogItemsFromCatalogBomRoot("caller-supplied YAML", yaml, bundle, result, null, true, ImmutableMap.of(), 0, forceUpdate, true);

        // do this at the end for atomic updates; if there are intra-yaml references, we handle them specially
        // (but for legacy items we only support them when using `item: { type: co-bundled-type }` syntax,
        // where the co-bundled type is declared previously; we do NOT support `item: { services: ... }` syntax for co-bundled refs
        for (CatalogItemDtoAbstract<?, ?> item: result) {
            if (bundle!=null && bundle.getVersionedName()!=null) {
                item.setContainingBundle(bundle.getVersionedName());
            }
            addItemDto(item, forceUpdate);
        }
        // do type validation so supertypes are populated and errors are at least logged in legacy mode (only time this is used)
        // (validation normally done by osgi load routines)
        Map<String,Collection<Throwable>> errors = MutableMap.of();
        for (CatalogItemDtoAbstract<?, ?> item: result) {
            Collection<Throwable> errorsInItem = validateType(RegisteredTypes.of(item), null, true);
            if (!errorsInItem.isEmpty()) {
                errors.put(item.getCatalogItemId(), errorsInItem);
            }
        }
        if (!errors.isEmpty()) {
            log.warn("Error adding YAML"+(bundle!=null ? " for bundle "+bundle : "")+" (ignoring, but types will not be usable): "+errors);
        }
        return result;
    }
    
    @Override @Beta
    public void addTypesFromBundleBom(String yaml, ManagedBundle bundle, boolean forceUpdate, Map<RegisteredType, RegisteredType> result) {
        log.debug("Catalog load, adding registered types to "+mgmt+" for bundle "+bundle+": "+yaml);
        checkNotNull(yaml, "yaml");
        if (result==null) result = MutableMap.of();
        collectCatalogItemsFromCatalogBomRoot("bundle BOM in "+bundle, yaml, bundle, null, result, false, MutableMap.of(), 0, forceUpdate, false);
    }

    @Override @Beta
    // mainly needed for tests which expect errors about item addition, which could be masked by errors on version clashes
    public Collection<RegisteredType> addTypesAndValidateAllowInconsistent(String catalogYaml, @Nullable Map<RegisteredType, RegisteredType> result, boolean forceUpdate) {
        checkNotNull(catalogYaml, "catalogYaml");

        Maybe<OsgiManager> osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager();
        if (osgiManager.isPresent() && AUTO_WRAP_CATALOG_YAML_AS_BUNDLE) {
            // wrap in a bundle to be managed; need to get bundle and version from yaml
            return addItemsOsgi(catalogYaml, forceUpdate, osgiManager.get()).getTypesInstalled();
            // above will have done validation and supertypes recorded
        }

        // often in tests we don't have osgi and so it acts as follows
        log.debug("Catalog load, adding registered types to "+mgmt+": "+catalogYaml);
        if (result==null) result = MutableMap.of();
        collectCatalogItemsFromCatalogBomRoot("unbundled catalog definition", catalogYaml, null, null, result, false, MutableMap.of(), 0, forceUpdate, true);

        Map<RegisteredType, Collection<Throwable>> validation = validateTypes(result.keySet());
        if (Iterables.concat(validation.values()).iterator().hasNext()) {
            throw new IllegalStateException("Could not validate one or more items: "+validation);
        }
        return validation.keySet();
    }

    @Override @Beta
    public Map<RegisteredType,Collection<Throwable>> validateTypes(Iterable<RegisteredType> typesToValidate) {
        return validateTypes(typesToValidate, false);
    }

    @Override @Beta
    public Map<RegisteredType,Collection<Throwable>> validateTypes(Iterable<RegisteredType> typesToValidate, boolean skipIfValidated) {
        List<RegisteredType> typesRemainingToValidate = MutableList.copyOf(typesToValidate);
        if (typesRemainingToValidate.isEmpty()) {
            return MutableMap.of();
        }
        log.debug("Starting validation, "+typesRemainingToValidate.size()+" to validate");
        while (true) {
            Map<RegisteredType,Collection<Throwable>> result = MutableMap.of();
            for (RegisteredType t: typesRemainingToValidate) {
                if (skipIfValidated && t.getKind() != null && t.getKind() != RegisteredTypeKind.UNRESOLVED) {
                    continue;  // probably validated as part of resolving another type
                }
                Collection<Throwable> tr = validateType(t, null, true);
                if (!tr.isEmpty()) {
                    result.put(t, tr);
                }
            }
            String msg = (typesRemainingToValidate.size()-result.size())+" validated, "+result.size()+" unvalidated";
            if (result.isEmpty() || result.size()==typesRemainingToValidate.size()) {
                log.debug("Finished validation, "+msg);
                return result;
            }
            log.debug("Finished validation cycle, "+msg+"; will re-run");
            // recurse wherever there were problems so long as we are reducing the number of problem types
            // (this lets us solve complex reference problems without needing a complex dependency tree,
            // in max O(N^2) time)
            typesRemainingToValidate = MutableList.copyOf(result.keySet());
        }
    }
    
    @Override @Beta
    public Collection<Throwable> validateType(RegisteredType typeToValidate, RegisteredTypeLoadingContext constraint, boolean allowUnresolved) {
        ReferenceWithError<RegisteredType> result = validateResolve(typeToValidate, constraint);
        if (result.hasError()) {
            if (allowUnresolved && RegisteredTypes.isTemplate(typeToValidate)) {
                // ignore for templates
                return Collections.emptySet();
            }
            if (result.getError() instanceof CompoundRuntimeException) {
                return ((CompoundRuntimeException)result.getError()).getAllCauses();
            }
            return Collections.singleton(result.getError());
        }
        // replace what's in catalog with resolved+validated version
        ((BasicBrooklynTypeRegistry) mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(result.get(), true);
        return Collections.emptySet();
    }

    /** 
     * Resolves the given object with respect to the catalog. Returns any errors found while trying to resolve. 
     * The argument may be changed (e.g. its kind set, supertypes set), and normal usage is to add 
     * a type in an "unresolved" state if things may need to reference it, then call resolve here,
     * then replace what was added with the argument given here. */
    ReferenceWithError<RegisteredType> validateResolve(RegisteredType typeToValidate, RegisteredTypeLoadingContext constraint) {
        Throwable inconsistentSuperTypesError=null, specError=null, beanError=null;
        List<Throwable> guesserErrors = MutableList.of();
        
        // collect supertype spec / most specific java
        Set<Object> supers = typeToValidate.getSuperTypes();
        BrooklynObjectType boType = null;
        for (Object superI: supers) {
            BrooklynObjectType boTypeI = null;
            if (superI instanceof BrooklynObject) boTypeI = BrooklynObjectType.of((BrooklynObject)superI);
            else if (superI instanceof Class) boTypeI = BrooklynObjectType.of((Class<?>)superI);
            if (boTypeI!=null && boTypeI!=BrooklynObjectType.UNKNOWN) {
                if (boType==null) boType = boTypeI;
                else {
                    if (boTypeI!=boType) {
                        inconsistentSuperTypesError = new IllegalStateException("Inconsistent supertypes for "+typeToValidate+"; indicates "+boType+" and "+boTypeI);
                    }
                }
            }
        }
        Class<?> superJ = null;
        for (Object superI: supers) {
            if (superI instanceof Class) {
                if (superJ==null) superJ = (Class<?>) superI;
                else if (superJ.isAssignableFrom((Class<?>)superI)) superJ = (Class<?>) superI;
            }
        }
        
        // could filter what we try based on kind; also could support itemType spec (generic) and 
        // more importantly bean to allow arbitrary types to be added to catalog
        
        RegisteredType resultT = null;
        
        Object resultO = null;
        if (resultO==null && boType!=null) try {
            // try spec instantiation if we know the BO Type (no point otherwise)
            resultT = RegisteredTypes.copyResolved(RegisteredTypeKind.SPEC, typeToValidate);
            try {
                resultO = ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).createSpec(resultT, constraint, boType.getSpecType());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                specError = e;
                resultT = null;
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // ignore if we couldn't resolve as spec
        }
        
        if (resultO==null) try {
            // try it as a bean
            resultT = RegisteredTypes.copyResolved(RegisteredTypeKind.BEAN, typeToValidate);
            try {
                resultO = ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).createBean(resultT, constraint, superJ);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                beanError = e;
                resultT = null;
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // ignore if we couldn't resolve as spec
        }
        
        if (resultO==null && (constraint==null || constraint.getAlreadyEncounteredTypes().isEmpty())) try {
            // try the messy but useful PlanInterpreterGuessingType
            // (that is the only place where we will guess specs, so it handles most of our traditional catalog items in BOMs);
            // but do not allow this to run if we are expanding a nested definition as that may fail to find recursive loops
            // (the legacy routines this uses don't support that type of context)
            String yaml = RegisteredTypes.getImplementationDataStringForSpec(typeToValidate);
            CatalogBundle bundle = typeToValidate.getContainingBundle() != null ? CatalogItemDtoAbstract.parseLibraries(Arrays.asList(typeToValidate.getContainingBundle())).iterator().next() : null;
            CatalogItemType itemType = boType!=null ? CatalogItemType.ofTargetClass(boType.getInterfaceType()) : null;
            if (itemType==null && typeToValidate.getKind() == RegisteredTypeKind.BEAN) {
                itemType = CatalogItemType.BEAN;
            }
            String format = typeToValidate.getPlan().getPlanFormat();
            PlanInterpreterInferringType guesser = new PlanInterpreterInferringType(typeToValidate.getSymbolicName(), Iterables.getOnlyElement( Yamls.parseAll(yaml) ),
                yaml, itemType, format, bundle, CatalogItemDtoAbstract.parseLibraries( typeToValidate.getLibraries() ), constraint, null);
            guesser.resolve();
            guesserErrors.addAll(guesser.getErrors());
            
            if (guesser.isResolved()) {
                // guesser resolved, but we couldn't create; did guesser change something?
                
                CatalogItemType ciType = guesser.getCatalogItemType();
                // try this even for templates; errors in them will be ignored by validator
                // but might be interesting to someone calling resolve directly
                
                boolean changedSomething = false;
                // reset resultT and change things as needed based on guesser
                resultT = typeToValidate;
                if (boType==null) {
                    // guesser inferred a type
                    boType = BrooklynObjectType.of(ciType);
                    if (boType!=null && boType.getSpecType()!=null) {
                        supers = MutableSet.copyOf(supers);
                        supers.add(boType.getInterfaceType());
                        // didn't know type before, retry now that we know the type
                        resultT = RegisteredTypes.copyResolved(RegisteredTypeKind.SPEC, resultT);
                        RegisteredTypes.addSuperTypes(resultT, supers);
                        changedSomething = true;
                    }
                }
                
                if (!Objects.equal(guesser.getPlanYaml(), yaml)) {
                    RegisteredTypes.changePlanNotingEquivalent(resultT, 
                        new BasicTypeImplementationPlan(typeToValidate.getPlan().getPlanFormat(), guesser.getPlanYaml()));
                    changedSomething = true;
                }
                
                if (changedSomething) {
                    // try again with new plan or supertype info
                    return validateResolve(resultT, constraint);
                    
                } else if (Objects.equal(boType, BrooklynObjectType.of(ciType))) {
                    if (specError==null) {
                        throw new IllegalStateException("Guesser resolved but TypeRegistry couldn't create");
                    } else {
                        // do nothing; type was already known, prefer the spec error
                    }
                } else {
                    throw new IllegalStateException("Guesser resolved as "+ciType+" but we expected "+boType);
                }
            } else {
                throw new IllegalStateException("Guesser could not resolve");
            }
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            guesserErrors.add(e);
            resultT = null;
        }

        if (resultO!=null && resultT!=null) {
            if (resultO instanceof BrooklynObject) {
                // if it was a bean that points at a BO then switch it to a spec and try to re-validate
                return validateResolve(RegisteredTypes.copyResolved(RegisteredTypeKind.SPEC, typeToValidate), constraint);
            }
            Class<?> resultS;
            if (resultT.getKind() == RegisteredTypeKind.SPEC) {
                resultS = ((AbstractBrooklynObjectSpec<?,?>)resultO).getType();
            } else {
                resultS = resultO.getClass();
            }
            RegisteredTypes.cacheActualJavaType(resultT, resultS);
            
            Set<Object> newSupers = MutableSet.of();
            // TODO collect registered type name supertypes, as strings
            newSupers.add(resultS);
            newSupers.addAll(supers);
            newSupers.add(BrooklynObjectType.of(resultO.getClass()).getInterfaceType());
            collectSupers(newSupers);
            RegisteredTypes.addSuperTypes(resultT, newSupers);

            return ReferenceWithError.newInstanceWithoutError(resultT);
        }
        
        List<Throwable> errors = MutableList.<Throwable>of()
            .appendIfNotNull(inconsistentSuperTypesError)
            .appendAll(guesserErrors)
            .appendIfNotNull(beanError)
            .appendIfNotNull(specError);
        return ReferenceWithError.newInstanceThrowingError(null, Exceptions.create("Could not resolve "+typeToValidate, errors));
    }

    private void collectSupers(Set<Object> s) {
        Queue<Object> remaining = new LinkedList<>();
        remaining.addAll(s);
        s.clear();
        while (!remaining.isEmpty()) {
            Object next = remaining.remove();
            if (next instanceof Class<?>) {
                if (!s.contains(next)) {
                    s.add(next);
                    remaining.add( ((Class<?>)next).getSuperclass() );
                    remaining.addAll( Arrays.asList( ((Class<?>)next).getInterfaces() ) );
                }
            }
        }
    }

    private CatalogItem<?,?> addItemDto(CatalogItemDtoAbstract<?, ?> itemDto, boolean forceUpdate) {
        CatalogItem<?, ?> existingDto = checkItemAllowedAndIfSoReturnAnyDuplicate(itemDto, true, forceUpdate);
        if (existingDto!=null) {
            // it's a duplicate, and not forced, just return it
            log.trace("Using existing duplicate for catalog item {}", itemDto.getId());
            return existingDto;
        }

        // Clear spec cache (in-case overwriting existing)
        specCache.invalidate();
        
        if (manualAdditionsCatalog==null) loadManualAdditionsCatalog();
        manualAdditionsCatalog.addEntry(itemDto);

        // Ensure the cache is populated and it is persisted by the management context
        getCatalog().addEntry(itemDto);

        // Request that the management context persist the item.
        if (log.isTraceEnabled()) {
            log.trace("Scheduling item for persistence addition: {}", itemDto.getId());
        }
        onAdditionUpdateOtherRegistries(itemDto);
        mgmt.getRebindManager().getChangeListener().onManaged(itemDto);

        return itemDto;
    }

    private void onAdditionUpdateOtherRegistries(CatalogItemDtoAbstract<?, ?> itemDto) {
        // nothing needed (previously updated BasicLocationRegistry but now that is a facade)
    }

    /** returns item DTO if item is an allowed duplicate, or null if it should be added (there is no duplicate), 
     * throwing if item cannot be added */
    private CatalogItem<?, ?> checkItemAllowedAndIfSoReturnAnyDuplicate(CatalogItem<?,?> itemDto, boolean allowDuplicates, boolean forceUpdate) {
        if (forceUpdate) return null;
        // Can update same snapshot version - very useful while developing blueprints
        if (itemDto.getVersion().contains("SNAPSHOT")) return null;
        CatalogItemDo<?, ?> existingItem = getCatalogItemDo(itemDto.getSymbolicName(), itemDto.getVersion());
        if (existingItem == null) return null;
        // check if they are equal
        CatalogItem<?, ?> existingDto = existingItem.getDto();
        if (existingDto.equals(itemDto)) {
            if (allowDuplicates) return existingItem;
            throw new IllegalStateException("Not allowed to update existing catalog entries, even with the same content: " +
                    itemDto.getSymbolicName() + ":" + itemDto.getVersion());
        } else {
            throw new IllegalStateException("Cannot add " + itemDto.getSymbolicName() + ":" + itemDto.getVersion() + 
                " to catalog; a different definition is already present");
        }
    }

    @Override @Deprecated /** @deprecated see super */
    public void addItem(CatalogItem<?,?> item) {
        // Clear spec-cache (in-case overwriting)
        specCache.invalidate();
        
        //assume forceUpdate for backwards compatibility
        log.debug("Adding manual catalog item to "+mgmt+": "+item);
        checkNotNull(item, "item");
        //don't activate bundles; only intended for legacy tests where that might not work
        CatalogUtils.installLibraries(mgmt, item.getLibraries(), false);
        if (manualAdditionsCatalog==null) loadManualAdditionsCatalog();
        manualAdditionsCatalog.addEntry(getAbstractCatalogItem(item));
    }

    @Override @Deprecated /** @deprecated see super */
    public void addCatalogLegacyItemsOnRebind(Iterable<? extends CatalogItem<?,?>> items) {
        addCatalogLegacyItemsOnRebind(items, true);
    }
    
    private void addCatalogLegacyItemsOnRebind(Iterable<? extends CatalogItem<?,?>> items, boolean failOnLoadError) {
        specCache.invalidate();
        
        log.debug("Adding manual catalog items to "+mgmt+": "+items);
        checkNotNull(items, "item");
        for (CatalogItem<?,?> item : items) {
            CatalogItemDtoAbstract<?,?> cdto;
            if (item instanceof CatalogItemDtoAbstract) {
                cdto = (CatalogItemDtoAbstract<?,?>) item;
            } else if (item instanceof CatalogItemDo && ((CatalogItemDo<?,?>)item).getDto() instanceof CatalogItemDtoAbstract) {
                cdto = (CatalogItemDtoAbstract<?,?>) ((CatalogItemDo<?,?>)item).getDto();
            } else {
                throw new IllegalArgumentException("Expected items of type "+CatalogItemDtoAbstract.class.getSimpleName());
            }
            cdto.setManagementContext((ManagementContextInternal) mgmt);
            
            try {
                CatalogUtils.installLibraries(mgmt, item.getLibraries());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                if (failOnLoadError) {
                    Exceptions.propagateAnnotated("Loading bundles for catalog item " + item.getCatalogItemId() + " failed", e);
                } else {
                    log.error("Loading bundles for catalog item " + item + " failed: " + e.getMessage(), e);
                }
            }
            catalog.addEntry((CatalogItemDtoAbstract<?,?>)item);
        }
        catalog.load(mgmt, null, failOnLoadError);
    }

    @Override @Deprecated /** @deprecated see super */
    public CatalogItem<?,?> addItem(Class<?> type) {
        //assume forceUpdate for backwards compatibility
        log.debug("Adding manual catalog item to "+mgmt+": "+type);
        checkNotNull(type, "type");
        if (manualAdditionsCatalog==null) loadManualAdditionsCatalog();
        manualAdditionsClasses.registerClass(type);
        CatalogItem<?, ?> result = manualAdditionsCatalog.classpath.addCatalogEntry(type);
        
        // Clear spec-cache (in-case overwriting)
        specCache.invalidate();
        
        return result;
    }

    private synchronized void loadManualAdditionsCatalog() {
        if (manualAdditionsCatalog!=null) return;
        CatalogDto manualAdditionsCatalogDto = CatalogDto.newNamedInstance(
                "Manual Catalog Additions", "User-additions to the catalog while Brooklyn is running, " +
                "created "+Time.makeDateString(),
                "manual-additions");
        CatalogDo manualAdditionsCatalog = catalog.addCatalog(manualAdditionsCatalogDto);
        if (manualAdditionsCatalog==null) {
            // not hard to support, but slightly messy -- probably have to use ID's to retrieve the loaded instance
            // for now block once, then retry
            log.warn("Blocking until catalog is loaded before changing it");
            boolean loaded = blockIfNotLoaded(Duration.TEN_SECONDS);
            if (!loaded)
                log.warn("Catalog still not loaded after delay; subsequent operations may fail");
            manualAdditionsCatalog = catalog.addCatalog(manualAdditionsCatalogDto);
            if (manualAdditionsCatalog==null) {
                throw new UnsupportedOperationException("Catalogs cannot be added until the base catalog is loaded, and catalog is taking a while to load!");
            }
        }
        
        log.debug("Creating manual additions catalog for "+mgmt+": "+manualAdditionsCatalog);
        manualAdditionsClasses = new LoadedClassLoader();
        ((AggregateClassLoader)manualAdditionsCatalog.classpath.getLocalClassLoader()).addFirst(manualAdditionsClasses);
        
        // expose when we're all done
        this.manualAdditionsCatalog = manualAdditionsCatalog;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Deprecated
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems() {
        Map<String,CatalogItem<T,SpecT>> result = MutableMap.of();
        if (!getCatalog().isLoaded()) {
            // some callers use this to force the catalog to load (maybe when starting as hot_backup without a catalog ?)
            log.debug("Forcing catalog load on access of catalog items");
            load();
        }
        for (RegisteredType rt: mgmt.getTypeRegistry().getAll()) {
            result.put(rt.getId(), (CatalogItem)RegisteredTypes.toPartialCatalogItem(rt));
        }
        // prefer locally registered items in this method; prevents conversion to and from RT;
        // possibly allows different views if there are diff items in catlaog and type registry
        // but this means at least it is consistent for user if they are consistent;
        // and can easily live with this until catalog is entirely replaced to TR 
        result.putAll((Map)catalog.getIdCache());
        return result.values();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    @Deprecated
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItemsLegacy() {
        if (!getCatalog().isLoaded()) {
            // some callers use this to force the catalog to load (maybe when starting as hot_backup without a catalog ?)
            log.debug("Forcing catalog load on access of catalog items");
            load();
        }
        return ImmutableList.copyOf((Iterable)catalog.getIdCache().values());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override @Deprecated
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems(Predicate<? super CatalogItem<T,SpecT>> filter) {
        Iterable<CatalogItem<T,SpecT>> filtered = Iterables.filter(getCatalogItems(), (Predicate) filter);
        return Iterables.transform(filtered, BasicBrooklynCatalog.<T,SpecT>itemDoToDto());
    }
    @Override @Deprecated
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItemsLegacy(Predicate<? super CatalogItem<T,SpecT>> filter) {
        Iterable<CatalogItemDo<T,SpecT>> filtered = Iterables.filter((Iterable)catalog.getIdCache().values(), (Predicate<CatalogItem<T,SpecT>>)(Predicate) filter);
        return Iterables.transform(filtered, BasicBrooklynCatalog.<T,SpecT>itemDoToDto());
    }

    private static <T,SpecT> Function<CatalogItem<T,SpecT>, CatalogItem<T,SpecT>> itemDoToDto() {
        return new Function<CatalogItem<T,SpecT>, CatalogItem<T,SpecT>>() {
            @Override
            public CatalogItem<T,SpecT> apply(@Nullable CatalogItem<T,SpecT> item) {
                if (!(item instanceof CatalogItemDo)) return item;
                return ((CatalogItemDo<T,SpecT>) item).getDto();
            }
        };
    }
    
    private static <T,SpecT> Function<CatalogItemDo<T, SpecT>, CatalogItem<T,SpecT>> itemDoToDtoAddingSelectedMetadataDuringScan(final ManagementContext mgmt, final Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        return new Function<CatalogItemDo<T,SpecT>, CatalogItem<T,SpecT>>() {
            @Override
            public CatalogItem<T,SpecT> apply(@Nullable CatalogItemDo<T,SpecT> item) {
                if (item==null) return null;
                CatalogItemDtoAbstract<T, SpecT> dto = (CatalogItemDtoAbstract<T, SpecT>) item.getDto();

                // allow metadata to overwrite version and library bundles;
                // however this should only be used for local classpath scanning and legacy external libraries;
                // bundle scans should _not_ use this
                
                String version = getFirstAs(catalogMetadata, String.class, "version").orNull();
                if (Strings.isNonBlank(version)) dto.setVersion(version);
                
                Collection<CatalogBundle> libraryBundles = MutableSet.of(); 
                if (!isNoBundleOrSimpleWrappingBundle(mgmt, containingBundle)) {
                    libraryBundles.add(new CatalogBundleDto(containingBundle.getSymbolicName(), containingBundle.getSuppliedVersionString(), null));
                }
                libraryBundles.addAll(dto.getLibraries());
                Object librariesInherited;
                librariesInherited = catalogMetadata.get("brooklyn.libraries");
                if (librariesInherited instanceof Collection) {
                    // will be set by scan -- slightly longwinded way to retrieve, but scanning java should be deprecated I think (AH)
                    libraryBundles.addAll(resolveWherePossible(mgmt, CatalogItemDtoAbstract.parseLibraries((Collection<?>) librariesInherited)));
                }
                librariesInherited = catalogMetadata.get("libraries");
                if (librariesInherited instanceof Collection) {
                    log.warn("Legacy 'libraries' encountered; use 'brooklyn.libraries'");
                    // will be set by scan -- slightly longwinded way to retrieve, but scanning for osgi needs an overhaul in any case
                    libraryBundles.addAll(resolveWherePossible(mgmt, CatalogItemDtoAbstract.parseLibraries((Collection<?>) librariesInherited)));
                }
                dto.setLibraries(libraryBundles);
                
                if (containingBundle!=null && dto.getContainingBundle()==null) {
                    dto.setContainingBundle(containingBundle.getVersionedName());
                }
                
                // replace java type with plan yaml -- needed for libraries / catalog item to be picked up,
                // but probably useful to transition away from javaType altogether
                dto.setSymbolicName(dto.getJavaType());
                switch (dto.getCatalogItemType()) {
                    case TEMPLATE:
                    case APPLICATION:
                    case ENTITY:
                        dto.setPlanYaml("services: [{ type: "+dto.getJavaType()+" }]");
                        break;
                    case POLICY:
                        dto.setPlanYaml(POLICIES_KEY + ": [{ type: "+dto.getJavaType()+" }]");
                        break;
                    case ENRICHER:
                        dto.setPlanYaml(ENRICHERS_KEY + ": [{ type: "+dto.getJavaType()+" }]");
                        break;
                    case LOCATION:
                        dto.setPlanYaml(LOCATIONS_KEY + ": [{ type: "+dto.getJavaType()+" }]");
                        break;
                    default:
                        throw new IllegalStateException("Not supported to create a catalog item " + dto.getCatalogItemId() + " from: "+dto.getCatalogItemType());
                }
                dto.setJavaType(null);

                return dto;
            }
        };
    }

    private static class SpecCache {
        private final Map<String, AbstractBrooklynObjectSpec<?,?>> cache = Collections.synchronizedMap(
                Maps.<String, AbstractBrooklynObjectSpec<?,?>>newLinkedHashMap());

        /**
         * Whenever anything in the catalog is modified, the entire cache should be invalidated. 
         * This is because items in the cache can refer to each other, which can impact the Spec
         * created for a given catalog item.
         */
        public void invalidate() {
            cache.clear();
        }
        
        public Optional<AbstractBrooklynObjectSpec<?,?>> getSpec(String itemId) {
            return Optional.<AbstractBrooklynObjectSpec<?,?>>fromNullable(cache.get(itemId));
        }
        
        public void addSpec(String itemId, AbstractBrooklynObjectSpec<?,?> spec) {
            cache.put(itemId, spec);
        }
    }
    
    private Object uninstallingEmptyLock = new Object();
    public void uninstallEmptyWrapperBundles() {
        log.debug("Uninstalling empty wrapper bundles");
        synchronized (uninstallingEmptyLock) {
            Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgi.isAbsent()) return;
            for (ManagedBundle b: osgi.get().getInstalledWrapperBundles()) {
                if (isNoBundleOrSimpleWrappingBundle(mgmt, b)) {
                    Iterable<RegisteredType> typesInBundle = osgi.get().getTypesFromBundle(b.getVersionedName());
                    if (Iterables.isEmpty(typesInBundle)) {
                        log.debug("Uninstalling now-empty BOM wrapper bundle "+b.getVersionedName()+" ("+b.getOsgiUniqueUrl()+")");
                        osgi.get().uninstallUploadedBundle(b);
                    }
                }
            }
        }
    }
    
}
