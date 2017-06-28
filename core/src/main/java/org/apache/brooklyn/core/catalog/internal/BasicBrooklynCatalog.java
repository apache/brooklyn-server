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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.CatalogClasspathDo.CatalogScanningModes;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.CampYamlParser;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.BrooklynTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypeNaming;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.AggregateClassLoader;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.LoadedClassLoader;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yaml.Yamls.YamlExtract;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/* TODO the complex tree-structured catalogs are only useful when we are relying on those separate catalog classloaders
 * to isolate classpaths. with osgi everything is just put into the "manual additions" catalog. */
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
        
        // TODO Stack, for recursive calls?
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
        for (CatalogItem<?, ?> toRemove : getCatalogItems()) {
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
        for (CatalogItem<?, ?> entry : getCatalogItems()) {
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

    @Override
    public CatalogItem<?,?> getCatalogItem(String symbolicName, String version) {
        if (symbolicName == null) return null;
        CatalogItemDo<?, ?> itemDo = getCatalogItemDo(symbolicName, version);
        if (itemDo == null) return null;
        return itemDo.getDto();
    }
    
    @Override
    public void deleteCatalogItem(String symbolicName, String version) {
        log.debug("Deleting manual catalog item from "+mgmt+": "+symbolicName + ":" + version);
        checkNotNull(symbolicName, "id");
        checkNotNull(version, "version");
        if (DEFAULT_VERSION.equals(version)) {
            throw new IllegalStateException("Deleting items with unspecified version (argument DEFAULT_VERSION) not supported.");
        }
        CatalogItem<?, ?> item = getCatalogItem(symbolicName, version);
        CatalogItemDtoAbstract<?,?> itemDto = getAbstractCatalogItem(item);
        if (itemDto == null) {
            throw new NoSuchElementException("No catalog item found with id "+symbolicName);
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
        if (itemDto.getCatalogItemType() == CatalogItemType.LOCATION) {
            @SuppressWarnings("unchecked")
            CatalogItem<Location,LocationSpec<?>> locationItem = (CatalogItem<Location, LocationSpec<?>>) itemDto;
            ((BasicLocationRegistry)mgmt.getLocationRegistry()).removeDefinedLocation(locationItem);
        }
        mgmt.getRebindManager().getChangeListener().onUnmanaged(itemDto);

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T,SpecT> CatalogItem<T,SpecT> getCatalogItem(Class<T> type, String id, String version) {
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
        if (loadedItem == null) throw new RuntimeException(item+" not in catalog; cannot create spec");
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
        throw new IllegalStateException("Cannot unwrap catalog item '"+item+"' (type "+item.getClass()+") to restore DTO");
    }
    
    @SuppressWarnings("unchecked")
    private static <T> Maybe<T> getFirstAs(Map<?,?> map, Class<T> type, String firstKey, String ...otherKeys) {
        if (map==null) return Maybe.absent("No map available");
        String foundKey = null;
        Object value = null;
        if (map.containsKey(firstKey)) foundKey = firstKey;
        else for (String key: otherKeys) {
            if (map.containsKey(key)) {
                foundKey = key;
                break;
            }
        }
        if (foundKey==null) return Maybe.absent("Missing entry '"+firstKey+"'");
        value = map.get(foundKey);
        if (type.equals(String.class) && Number.class.isInstance(value)) value = value.toString();
        if (!type.isInstance(value)) 
            throw new IllegalArgumentException("Entry for '"+firstKey+"' should be of type "+type+", not "+(value==null ? "null" : value.getClass()));
        return Maybe.of((T)value);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Maybe<Map<?,?>> getFirstAsMap(Map<?,?> map, String firstKey, String ...otherKeys) {
        return (Maybe) getFirstAs(map, Map.class, firstKey, otherKeys);
    }

    private List<CatalogItemDtoAbstract<?,?>> collectCatalogItems(String yaml, ManagedBundle containingBundle) {
        List<CatalogItemDtoAbstract<?, ?>> result = MutableList.of();
        collectCatalogItems(yaml, containingBundle, result, ImmutableMap.of());
        return result;
    }

    public static Map<?,?> getCatalogMetadata(String yaml) {
        Map<?,?> itemDef = Yamls.getAs(Yamls.parseAll(yaml), Map.class);
        return getFirstAsMap(itemDef, "brooklyn.catalog").orNull();        
    }
    
    /** @deprecated since 0.12.0 - use {@link #getVersionedName(Map, boolean)} */
    @Deprecated
    public static VersionedName getVersionedName(Map<?,?> catalogMetadata) {
        return getVersionedName(catalogMetadata, true);
    }
    
    public static VersionedName getVersionedName(Map<?,?> catalogMetadata, boolean required) {
        String version = getFirstAs(catalogMetadata, String.class, "version").orNull();
        String bundle = getFirstAs(catalogMetadata, String.class, "bundle").orNull();
        if (Strings.isBlank(bundle) && Strings.isBlank(version)) {
            if (!required) return null;
            throw new IllegalStateException("Catalog BOM must define bundle and version");
        }
        if (Strings.isBlank(bundle)) {
            if (!required) return null;
            throw new IllegalStateException("Catalog BOM must define bundle");
        }
        if (Strings.isBlank(version)) {
            throw new IllegalStateException("Catalog BOM must define version if bundle is defined");
        }
        return new VersionedName(bundle, version);
    }

    private void collectCatalogItems(String yaml, ManagedBundle containingBundle, List<CatalogItemDtoAbstract<?, ?>> result, Map<?, ?> parentMeta) {
        Map<?,?> itemDef = Yamls.getAs(Yamls.parseAll(yaml), Map.class);
        Map<?,?> catalogMetadata = getFirstAsMap(itemDef, "brooklyn.catalog").orNull();
        if (catalogMetadata==null)
            log.warn("No `brooklyn.catalog` supplied in catalog request; using legacy mode for "+itemDef);
        catalogMetadata = MutableMap.copyOf(catalogMetadata);

        collectCatalogItems(Yamls.getTextOfYamlAtPath(yaml, "brooklyn.catalog").getMatchedYamlTextOrWarn(), 
            containingBundle, catalogMetadata, result, parentMeta, 0);
        
        itemDef.remove("brooklyn.catalog");
        catalogMetadata.remove("item");
        catalogMetadata.remove("items");
        if (!itemDef.isEmpty()) {
            log.debug("Reading brooklyn.catalog peer keys as item ('top-level syntax')");
            Map<String,?> rootItem = MutableMap.of("item", itemDef);
            String rootItemYaml = yaml;
            YamlExtract yamlExtract = Yamls.getTextOfYamlAtPath(rootItemYaml, "brooklyn.catalog");
            String match = yamlExtract.withOriginalIndentation(true).withKeyIncluded(true).getMatchedYamlTextOrWarn();
            if (match!=null) {
                if (rootItemYaml.startsWith(match)) rootItemYaml = Strings.removeFromStart(rootItemYaml, match);
                else rootItemYaml = Strings.replaceAllNonRegex(rootItemYaml, "\n"+match, "");
            }
            collectCatalogItems("item:\n"+makeAsIndentedObject(rootItemYaml), containingBundle, rootItem, result, catalogMetadata, 1);
        }
    }

    @SuppressWarnings("unchecked")
    private void collectCatalogItems(String sourceYaml, ManagedBundle containingBundle, Map<?,?> itemMetadata, List<CatalogItemDtoAbstract<?, ?>> result, Map<?,?> parentMetadata, int depth) {

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
                itemMetadataWithoutItemDef = (Map<String, Object>) Tasks.resolveDeepValue(itemMetadataWithoutItemDef, Object.class, mgmt.getServerExecutionContext());
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

        // brooklyn.libraries we treat specially, to append the list, with the child's list preferred in classloading order
        // `libraries` is supported in some places as a legacy syntax; it should always be `brooklyn.libraries` for new apps
        // TODO in 0.8.0 require brooklyn.libraries, don't allow "libraries" on its own
        List<?> librariesAddedHereNames = MutableList.copyOf(getFirstAs(itemMetadataWithoutItemDef, List.class, "brooklyn.libraries", "libraries").orNull());
        Collection<CatalogBundle> librariesAddedHereBundles = CatalogItemDtoAbstract.parseLibraries(librariesAddedHereNames);
        
        MutableSet<Object> librariesCombinedNames = MutableSet.of();
        if (!isNoBundleOrSimpleWrappingBundle(containingBundle)) {
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

        Boolean scanJavaAnnotations = getFirstAs(itemMetadataWithoutItemDef, Boolean.class, "scanJavaAnnotations", "scan_java_annotations").orNull();
        if (scanJavaAnnotations==null || !scanJavaAnnotations) {
            // don't scan
        } else {
            if (isNoBundleOrSimpleWrappingBundle(containingBundle)) {
                // BOMs wrapped in JARs, or without JARs, have special treatment
                if (isLibrariesMoreThanJustContainingBundle(librariesAddedHereBundles, containingBundle)) {
                    // legacy mode, since 0.12.0, scan libraries referenced in a legacy non-bundle BOM
                    log.warn("Deprecated use of scanJavaAnnotations to scan other libraries ("+librariesAddedHereBundles+"); libraries should declare they scan themselves");
                    result.addAll(scanAnnotationsLegacyInListOfLibraries(mgmt, librariesAddedHereBundles, catalogMetadata, containingBundle));
                } else if (!isLibrariesMoreThanJustContainingBundle(libraryBundles, containingBundle)) {
                    // for default catalog, no libraries declared, we want to scan local classpath
                    // bundle should be named "brooklyn-default-catalog"
                    if (containingBundle!=null && !containingBundle.getSymbolicName().contains("brooklyn-default-catalog")) {
                        // a user uplaoded a BOM trying to tell us to do a local java scan; previously supported but becoming unsupported
                        log.warn("Deprecated use of scanJavaAnnotations in non-Java BOM outwith the default catalog setup"); 
                    } else if (depth>0) {
                        // since 0.12.0, require this to be right next to where libraries are defined, or at root
                        log.warn("Deprecated use of scanJavaAnnotations declared in item; should be declared at the top level of the BOM");
                    }
                    result.addAll(scanAnnotationsFromLocalNonBundleClasspath(mgmt, catalogMetadata, containingBundle));
                } else {
                    throw new IllegalStateException("Cannot scan for Java catalog items when libraries declared on an ancestor; scanJavaAnnotations should be specified alongside brooklyn.libraries (or ideally those libraries should specify to scan)");
                }
            } else {
                throw new IllegalArgumentException("Scanning for Java annotations is not supported in BOMs in bundles; "
                    + "entries should be listed explicitly in the catalog.bom");
                // see comments on scanAnnotationsInBundle
//                if (depth>0) {
//                    // since 0.12.0, require this to be right next to where libraries are defined, or at root
//                    log.warn("Deprecated use of scanJavaAnnotations declared in item; should be declared at the top level of the BOM");
//                }
//                // normal JAR install, only scan that bundle (the one containing the catalog.bom)
//                // note metadata not relevant here
//                result.addAll(scanAnnotationsInBundle(mgmt, containingBundle));
            }
        }
        
        Object items = catalogMetadata.remove("items");
        Object item = catalogMetadata.remove("item");
        Object url = catalogMetadata.remove("include");

        if (items!=null) {
            int count = 0;
            for (Object ii: checkType(items, "items", List.class)) {
                if (ii instanceof String) {
                    collectUrlReferencedCatalogItems((String) ii, containingBundle, result, catalogMetadata);
                } else {
                    Map<?,?> i = checkType(ii, "entry in items list", Map.class);
                    collectCatalogItems(Yamls.getTextOfYamlAtPath(sourceYaml, "items", count).getMatchedYamlTextOrWarn(),
                            containingBundle, i, result, catalogMetadata, depth+1);
                }
                count++;
            }
        }

        if (url != null) {
            collectUrlReferencedCatalogItems(checkType(url, "include in catalog meta", String.class), containingBundle, result, catalogMetadata);
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

        if ((Strings.isNonBlank(id) || Strings.isNonBlank(symbolicName)) && 
                Strings.isNonBlank(displayName) &&
                Strings.isNonBlank(name) && !name.equals(displayName)) {
            log.warn("Name property will be ignored due to the existence of displayName and at least one of id, symbolicName");
        }

        PlanInterpreterGuessingType planInterpreter = new PlanInterpreterGuessingType(null, item, sourceYaml, itemType, libraryBundles, result).reconstruct();
        if (!planInterpreter.isResolved()) {
            throw Exceptions.create("Could not resolve definition of item"
                + (Strings.isNonBlank(id) ? " '"+id+"'" : Strings.isNonBlank(symbolicName) ? " '"+symbolicName+"'" : Strings.isNonBlank(name) ? " '"+name+"'" : "")
                // better not to show yaml, takes up lots of space, and with multiple plan transformers there might be multiple errors; 
                // some of the errors themselves may reproduce it
                // (ideally in future we'll be able to return typed errors with caret position of error)
//                + ":\n"+sourceYaml
                , planInterpreter.getErrors());
        }
        itemType = planInterpreter.getCatalogItemType();
        Map<?, ?> itemAsMap = planInterpreter.getItem();
        // the "plan yaml" includes the services: ... or brooklyn.policies: ... outer key,
        // as opposed to the rawer { type: foo } map without that outer key which is valid as item input
        // TODO this plan yaml is needed for subsequent reconstruction; would be nicer if it weren't! 

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

        // icon.url is discouraged, but kept for legacy compatibility; should deprecate this
        final String catalogIconUrl = getFirstAs(catalogMetadata, String.class, "iconUrl", "icon_url", "icon.url").orNull();

        final String deprecated = getFirstAs(catalogMetadata, String.class, "deprecated").orNull();
        final Boolean catalogDeprecated = Boolean.valueOf(deprecated);

        // run again now that we know the ID
        planInterpreter = new PlanInterpreterGuessingType(id, item, sourceYaml, itemType, libraryBundles, result).reconstruct();
        if (!planInterpreter.isResolved()) {
            throw new IllegalStateException("Could not resolve plan once id and itemType are known (recursive reference?): "+sourceYaml);
        }
        String sourcePlanYaml = planInterpreter.getPlanYaml();

        CatalogItemDtoAbstract<?, ?> dto = createItemBuilder(itemType, symbolicName, version)
            .libraries(libraryBundles)
            .displayName(displayName)
            .description(description)
            .deprecated(catalogDeprecated)
            .iconUrl(catalogIconUrl)
            .plan(sourcePlanYaml)
            .build();

        dto.setManagementContext((ManagementContextInternal) mgmt);
        result.add(dto);
    }

    private boolean isLibrariesMoreThanJustContainingBundle(Collection<CatalogBundle> library, ManagedBundle containingBundle) {
        if (library==null) return false;
        if (containingBundle==null) return !library.isEmpty();
        if (library.size()>1) return true;
        CatalogBundle li = Iterables.getOnlyElement(library);
        return !containingBundle.getVersionedName().equalsOsgi(li.getVersionedName());
    }

    private boolean isNoBundleOrSimpleWrappingBundle(ManagedBundle b) {
        if (b==null) return true;
        String wrapped = ((ManagementContextInternal)mgmt).getOsgiManager().get().findBundle(b).get().getHeaders().get(BROOKLYN_WRAPPED_BOM_BUNDLE);
        return wrapped!=null && wrapped.equalsIgnoreCase("true");
    }

    private void collectUrlReferencedCatalogItems(String url, ManagedBundle containingBundle, List<CatalogItemDtoAbstract<?, ?>> result, Map<Object, Object> parentMeta) {
        @SuppressWarnings("unchecked")
        List<?> parentLibrariesRaw = MutableList.copyOf(getFirstAs(parentMeta, List.class, "brooklyn.libraries", "libraries").orNull());
        Collection<CatalogBundle> parentLibraries = CatalogItemDtoAbstract.parseLibraries(parentLibrariesRaw);
        BrooklynClassLoadingContext loader = CatalogUtils.newClassLoadingContext(mgmt, "<catalog url reference loader>:0.0.0", parentLibraries);
        String yaml;
        try {
            yaml = ResourceUtils.create(loader).getResourceAsString(url);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new IllegalStateException("Remote catalog url " + url + " can't be fetched.", e);
        }
        collectCatalogItems(yaml, containingBundle, result, parentMeta);
    }

    @SuppressWarnings("unchecked")
    private <T> T checkType(Object x, String description, Class<T> type) {
        if (type.isInstance(x)) return (T)x;
        throw new UserFacingException("Expected "+JavaClassNames.superSimpleClassName(type)+" for "+description+", not "+JavaClassNames.superSimpleClassName(x));
    }

    private String setFromItemIfUnset(String oldValue, Map<?,?> item, String fieldAttr) {
        if (Strings.isNonBlank(oldValue)) return oldValue;
        if (item!=null) {
            Object newValue = item.get(fieldAttr);
            if (newValue instanceof String && Strings.isNonBlank((String)newValue)) 
                return (String)newValue;
        }
        return oldValue;
    }

    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsFromLocalNonBundleClasspath(ManagementContext mgmt, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Local Scanned Catalog", "All annotated Brooklyn entities detected in the classpath", "scanning-local-classpath");
        return scanAnnotationsInternal(mgmt, new CatalogDo(dto), catalogMetadata, containingBundle);
    }
    
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsLegacyInListOfLibraries(ManagementContext mgmt, Collection<? extends OsgiBundleWithUrl> libraries, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Bundles Scanned Catalog", "All annotated Brooklyn entities detected in bundles", "scanning-bundles-classpath-"+libraries.hashCode());
        List<String> urls = MutableList.of();
        for (OsgiBundleWithUrl b: libraries) {
            // TODO currently does not support pre-installed bundles identified by name:version 
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
    
    @SuppressWarnings("unused")  // keep during 0.12.0 until we are decided we won't support this; search for this method name
    // note that it breaks after rebind since we don't have the JAR -- see notes below
    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsInBundle(ManagementContext mgmt, ManagedBundle containingBundle) {
        CatalogDto dto = CatalogDto.newNamedInstance("Bundle "+containingBundle.getVersionedName().toOsgiString()+" Scanned Catalog", "All annotated Brooklyn entities detected in bundles", "scanning-bundle-"+containingBundle.getVersionedName().toOsgiString());
        CatalogDo subCatalog = new CatalogDo(dto);
        // need access to a JAR to scan this
        String url = null;
        if (containingBundle instanceof BasicManagedBundle) {
            File f = ((BasicManagedBundle)containingBundle).getTempLocalFileWhenJustUploaded();
            if (f!=null) {
                url = "file:"+f.getAbsolutePath();
            }
        }
        // type.getSubPathName(), type, id+".jar", com.google.common.io.Files.asByteSource(f), exceptionHandler);
        if (url==null) {
            url = containingBundle.getUrl();
        }
        if (url==null) {
            // NOT available after persistence/rebind 
            // as shown by test in CatalogOsgiVersionMoreEntityRebindTest
            throw new IllegalArgumentException("Error prepaing to scan "+containingBundle.getVersionedName()+": no URL available");
        }
        // org.reflections requires the URL to be "file:" containg ".jar"
        File fJar = Os.newTempFile(containingBundle.getVersionedName().toOsgiString(), ".jar");
        try {
            Streams.copy(ResourceUtils.create().getResourceFromUrl(url), new FileOutputStream(fJar));
        } catch (FileNotFoundException e) {
            throw Exceptions.propagate("Error extracting "+url+" to scan "+containingBundle.getVersionedName(), e);
        }
        subCatalog.addToClasspath(new String[] { "file:"+fJar.getAbsolutePath() });
        Collection<CatalogItemDtoAbstract<?, ?>> result = scanAnnotationsInternal(mgmt, subCatalog, MutableMap.of("version", containingBundle.getSuppliedVersionString()), containingBundle);
        fJar.delete();
        return result;
    }

    private Collection<CatalogItemDtoAbstract<?, ?>> scanAnnotationsInternal(ManagementContext mgmt, CatalogDo subCatalog, Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
        subCatalog.mgmt = mgmt;
        subCatalog.setClasspathScanForEntities(CatalogScanningModes.ANNOTATIONS);
        subCatalog.load();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Collection<CatalogItemDtoAbstract<?, ?>> result = (Collection)Collections2.transform(
                (Collection<CatalogItemDo<Object,Object>>)(Collection)subCatalog.getIdCache().values(), 
                itemDoToDtoAddingSelectedMetadataDuringScan(catalogMetadata, containingBundle));
        return result;
    }

    private class PlanInterpreterGuessingType {

        final String id;
        final Map<?,?> item;
        final String itemYaml;
        final Collection<CatalogBundle> libraryBundles;
        final List<CatalogItemDtoAbstract<?, ?>> itemsDefinedSoFar;
        
        CatalogItemType catalogItemType;
        String planYaml;
        boolean resolved = false;
        List<Exception> errors = MutableList.of();
        List<Exception> entityErrors = MutableList.of();
        
        public PlanInterpreterGuessingType(@Nullable String id, Object item, String itemYaml, @Nullable CatalogItemType optionalCiType, 
                Collection<CatalogBundle> libraryBundles, List<CatalogItemDtoAbstract<?,?>> itemsDefinedSoFar) {
            // ID is useful to prevent recursive references (possibly only supported for entities?)
            this.id = id;
            
            if (item instanceof String) {
                // if just a string supplied, wrap as map
                this.item = MutableMap.of("type", item);
                this.itemYaml = "type:\n"+makeAsIndentedObject(itemYaml);                
            } else {
                this.item = (Map<?,?>)item;
                this.itemYaml = itemYaml;
            }
            this.catalogItemType = optionalCiType;
            this.libraryBundles = libraryBundles;
            this.itemsDefinedSoFar = itemsDefinedSoFar;
        }

        public PlanInterpreterGuessingType reconstruct() {
            if (catalogItemType==CatalogItemType.TEMPLATE) {
                // template *must* be explicitly defined, and if so, none of the other calls apply
                attemptType(null, CatalogItemType.TEMPLATE);
                
            } else {
                attemptType(null, CatalogItemType.ENTITY);

                attemptType("services", CatalogItemType.ENTITY);
                attemptType(POLICIES_KEY, CatalogItemType.POLICY);
                attemptType(ENRICHERS_KEY, CatalogItemType.ENRICHER);
                attemptType(LOCATIONS_KEY, CatalogItemType.LOCATION);
            }
            
            if (!resolved && catalogItemType==CatalogItemType.TEMPLATE) {
                // anything goes, for an explicit template, because we can't easily recurse into the types
                planYaml = itemYaml;
                resolved = true;
            }
            
            return this;
        }

        public boolean isResolved() { return resolved; }
        
        /** Returns potentially useful errors encountered while guessing types. 
         * May only be available where the type is known. */
        public List<Exception> getErrors() {
            if (errors.isEmpty()) return entityErrors;
            return errors;
        }
        
        public CatalogItemType getCatalogItemType() {
            return catalogItemType; 
        }
        
        public String getPlanYaml() {
            return planYaml;
        }
        
        private boolean attemptType(String key, CatalogItemType candidateCiType) {
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
            // first look in collected items, if a key is given
            String type = (String) item.get("type");
            
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
            {
                // legacy routine; should be the same as above code added in 0.12 because:
                // if type is symbolic_name, the type will match below, and version will be null so any version allowed to match 
                // if type is symbolic_name:version, the id will match, and the version will also have to match 
                // SHOULD NEVER NEED THIS - remove during or after 0.13
                String typeWithId = type;
                String version = null;
                if (CatalogUtils.looksLikeVersionedId(type)) {
                    version = CatalogUtils.getVersionFromVersionedId(type);
                    type = CatalogUtils.getSymbolicNameFromVersionedId(type);
                }
                if (type!=null && key!=null) {
                    for (CatalogItemDtoAbstract<?,?> candidate: itemsDefinedSoFar) {
                        if (candidateCiType == candidate.getCatalogItemType() &&
                                (type.equals(candidate.getSymbolicName()) || type.equals(candidate.getId()))) {
                            if (version==null || version.equals(candidate.getVersion())) {
                                log.warn("Lookup of '"+type+"' version '"+version+"' only worked using legacy routines; please advise Brooklyn community so they understand why");
                                // matched - exit
                                catalogItemType = candidateCiType;
                                planYaml = candidateYaml;
                                resolved = true;
                                return true;
                            }
                        }
                    }
                }
                
                type = typeWithId;
                // above line is a change to behaviour; previously we proceeded below with the version dropped in code above;
                // but that seems like a bug as the code below will have ignored version.
                // likely this means we are now stricter about loading things that reference new versions, but correctly so. 
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
                if (item.containsKey("services") && (candidateCiType==CatalogItemType.ENTITY || candidateCiType==CatalogItemType.TEMPLATE)) {
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
            return id != null ? id : Strings.makeRandomId(10);
        }
        public Map<?,?> getItem() {
            return item;
        }
    }
    
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

    // these kept as their logic may prove useful; Apr 2015
//    private boolean isApplicationSpec(EntitySpec<?> spec) {
//        return !Boolean.TRUE.equals(spec.getConfig().get(EntityManagementUtils.WRAPPER_APP_MARKER));
//    }
//
//    private boolean isEntityPlan(DeploymentPlan plan) {
//        return plan!=null && !plan.getServices().isEmpty() || !plan.getArtifacts().isEmpty();
//    }
//    
//    private boolean isPolicyPlan(DeploymentPlan plan) {
//        return !isEntityPlan(plan) && plan.getCustomAttributes().containsKey(POLICIES_KEY);
//    }
//
//    private boolean isLocationPlan(DeploymentPlan plan) {
//        return !isEntityPlan(plan) && plan.getCustomAttributes().containsKey(LOCATIONS_KEY);
//    }

    //------------------------
    
    @Override
    public CatalogItem<?,?> addItem(String yaml) {
        return addItem(yaml, false);
    }

    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml) {
        return addItems(yaml, false);
    }
    
    @Override
    public CatalogItem<?,?> addItem(String yaml, boolean forceUpdate) {
        return Iterables.getOnlyElement(addItems(yaml, forceUpdate));
    }
    
    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml, boolean forceUpdate) {
        Maybe<OsgiManager> osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager();
        if (osgiManager.isPresent() && AUTO_WRAP_CATALOG_YAML_AS_BUNDLE) {
            // wrap in a bundle to be managed; need to get bundle and version from yaml
            Map<?, ?> cm = BasicBrooklynCatalog.getCatalogMetadata(yaml);
            VersionedName vn = BasicBrooklynCatalog.getVersionedName( cm, false );
            if (vn==null) {
                // for better legacy compatibiity, if id specified at root use that
                String id = (String) cm.get("id");
                if (Strings.isNonBlank(id)) {
                    vn = VersionedName.fromString(id);
                }
                vn = new VersionedName(vn!=null && Strings.isNonBlank(vn.getSymbolicName()) ? vn.getSymbolicName() : "brooklyn-catalog-bom-"+Identifiers.makeRandomId(8), 
                    vn!=null && vn.getVersionString()!=null ? vn.getVersionString() : getFirstAs(cm, String.class, "version").or(NO_VERSION));
            }
            log.debug("Wrapping supplied BOM as "+vn);
            Manifest mf = new Manifest();
            mf.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, vn.getSymbolicName());
            mf.getMainAttributes().putValue(Constants.BUNDLE_VERSION, vn.getOsgiVersionString() );
            mf.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), OSGI_MANIFEST_VERSION_VALUE);
            mf.getMainAttributes().putValue(BROOKLYN_WRAPPED_BOM_BUNDLE, Boolean.TRUE.toString());
            
            BundleMaker bm = new BundleMaker(mgmt);
            File bf = bm.createTempBundle(vn.getSymbolicName(), mf, MutableMap.of(
                new ZipEntry(CATALOG_BOM), (InputStream) new ByteArrayInputStream(yaml.getBytes())) );

            OsgiBundleInstallationResult result = null;
            try {
                result = osgiManager.get().install(null, new FileInputStream(bf), true, true, forceUpdate).get();
            } catch (FileNotFoundException e) {
                throw Exceptions.propagate(e);
            }
            bf.delete();
            uninstallEmptyWrapperBundles();
            if (result.getCode().isError()) {
                throw new IllegalStateException(result.getMessage());
            }
            return toItems(result.getCatalogItemsInstalled());
            
            // TODO check if we've overridden all items pertaining to an older anonymous catalog.bom bundle
            // we could remove references to that anonymous bundle; 
            // without this currently we leak bundles as bom's are replaced
            // (because we persist each item as well as the bundle, and we use the item XML on rebind, 
            // rather than rereading the catalog.bom from the bundle, there isn't currently a risk of loading
            // any of those overwritten items; however probably wise in future to require a bundle ID)
        }
        // fallback to non-OSGi for tests and other environments
        return addItems(yaml, null, forceUpdate);
    }
    
    @SuppressWarnings("deprecation")
    private List<CatalogItem<?,?>> toItems(Iterable<String> itemIds) {
        List<CatalogItem<?,?>> result = MutableList.of();
        for (String id: itemIds) {
            // TODO prefer to use RegisteredType, but that's an API change here
            result.add(CatalogUtils.getCatalogItemOptionalVersion(mgmt, id));
        }
        return result;
    }
    
    @Override
    public List<? extends CatalogItem<?, ?>> addItems(String yaml, ManagedBundle bundle) {
        return addItems(yaml, bundle, false);
    }
    
    @Override
    public List<? extends CatalogItem<?,?>> addItems(String yaml, ManagedBundle bundle, boolean forceUpdate) {
        log.debug("Adding manual catalog item to "+mgmt+": "+yaml);
        checkNotNull(yaml, "yaml");
        List<CatalogItemDtoAbstract<?, ?>> result = collectCatalogItems(yaml, bundle);

        // do this at the end for atomic updates; if there are intra-yaml references, we handle them specially
        for (CatalogItemDtoAbstract<?, ?> item: result) {
            if (bundle!=null && bundle.getVersionedName()!=null) {
                item.setContainingBundle(bundle.getVersionedName());
            }
            addItemDto(item, forceUpdate);
        }
        return result;
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
        if (itemDto.getCatalogItemType() == CatalogItemType.LOCATION) {
            @SuppressWarnings("unchecked")
            CatalogItem<Location,LocationSpec<?>> locationItem = (CatalogItem<Location, LocationSpec<?>>) itemDto;
            ((BasicLocationRegistry)mgmt.getLocationRegistry()).updateDefinedLocation(locationItem);
        }
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
            throw new IllegalStateException("Updating existing catalog entries, even with the same content, is forbidden: " +
                    itemDto.getSymbolicName() + ":" + itemDto.getVersion() + ". Use forceUpdate argument to override.");
        } else {
            throw new IllegalStateException("Updating existing catalog entries is forbidden: " +
                    itemDto.getSymbolicName() + ":" + itemDto.getVersion() + ". Use forceUpdate argument to override.");
        }
    }

    @Override @Deprecated /** @deprecated see super */
    public void addItem(CatalogItem<?,?> item) {
        // Clear spec-cache (in-case overwriting)
        specCache.invalidate();
        
        //assume forceUpdate for backwards compatibility
        log.debug("Adding manual catalog item to "+mgmt+": "+item);
        checkNotNull(item, "item");
        CatalogUtils.installLibraries(mgmt, item.getLibraries());
        if (manualAdditionsCatalog==null) loadManualAdditionsCatalog();
        manualAdditionsCatalog.addEntry(getAbstractCatalogItem(item));
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
    @Override
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems() {
        if (!getCatalog().isLoaded()) {
            // some callers use this to force the catalog to load (maybe when starting as hot_backup without a catalog ?)
            log.debug("Forcing catalog load on access of catalog items");
            load();
        }
        return ImmutableList.copyOf((Iterable)catalog.getIdCache().values());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems(Predicate<? super CatalogItem<T,SpecT>> filter) {
        Iterable<CatalogItemDo<T,SpecT>> filtered = Iterables.filter((Iterable)catalog.getIdCache().values(), (Predicate<CatalogItem<T,SpecT>>)(Predicate) filter);
        return Iterables.transform(filtered, BasicBrooklynCatalog.<T,SpecT>itemDoToDto());
    }

    private static <T,SpecT> Function<CatalogItemDo<T,SpecT>, CatalogItem<T,SpecT>> itemDoToDto() {
        return new Function<CatalogItemDo<T,SpecT>, CatalogItem<T,SpecT>>() {
            @Override
            public CatalogItem<T,SpecT> apply(@Nullable CatalogItemDo<T,SpecT> item) {
                if (item==null) return null;
                return item.getDto();
            }
        };
    }
    
    private static <T,SpecT> Function<CatalogItemDo<T, SpecT>, CatalogItem<T,SpecT>> itemDoToDtoAddingSelectedMetadataDuringScan(final Map<?, ?> catalogMetadata, ManagedBundle containingBundle) {
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
                if (containingBundle!=null) {
                    libraryBundles.add(new CatalogBundleDto(containingBundle.getSymbolicName(), containingBundle.getSuppliedVersionString(), null));
                }
                libraryBundles.addAll(dto.getLibraries());
                Object librariesInherited;
                librariesInherited = catalogMetadata.get("brooklyn.libraries");
                if (librariesInherited instanceof Collection) {
                    // will be set by scan -- slightly longwinded way to retrieve, but scanning for osgi needs an overhaul in any case
                    libraryBundles.addAll(CatalogItemDtoAbstract.parseLibraries((Collection<?>) librariesInherited));
                }
                librariesInherited = catalogMetadata.get("libraries");
                if (librariesInherited instanceof Collection) {
                    log.warn("Legacy 'libraries' encountered; use 'brooklyn.libraries'");
                    // will be set by scan -- slightly longwinded way to retrieve, but scanning for osgi needs an overhaul in any case
                    libraryBundles.addAll(CatalogItemDtoAbstract.parseLibraries((Collection<?>) librariesInherited));
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
        log.debug("uninstalling empty wrapper bundles");
        synchronized (uninstallingEmptyLock) {
            Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgi.isAbsent()) return;
            for (ManagedBundle b: osgi.get().getManagedBundles().values()) {
                if (isNoBundleOrSimpleWrappingBundle(b)) {
                    Iterable<RegisteredType> typesInBundle = osgi.get().getTypesFromBundle(b.getVersionedName());
                    if (Iterables.isEmpty(typesInBundle)) {
                        log.debug("uninstalling empty wrapper bundle "+b);
                        osgi.get().uninstallUploadedBundle(b);
                    }
                }
            }
        }
    }
    
}
