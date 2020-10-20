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
package org.apache.brooklyn.api.catalog;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;

public interface BrooklynCatalog {
    /** 
     * Version set in catalog when creator does not supply a version, to mean a low priority item;
     * and used when requesting to indicate the best version.
     * (See {@link #getCatalogItem(String, String)} for discussion of the best version.)
     */
    static String DEFAULT_VERSION = "0.0.0_DEFAULT_VERSION";

    /** @return The item matching the given given 
     * {@link CatalogItem#getSymbolicName() symbolicName} 
     * and optionally {@link CatalogItem#getVersion()},
     * taking the best version if the version is {@link #DEFAULT_VERSION} or null,
     * returning null if no matches are found. 
     * @deprecated since 0.12.0 use {@link BrooklynTypeRegistry} instead */ 
    @Deprecated
    CatalogItem<?,?> getCatalogItem(String symbolicName, String version);
    
    /** As {@link #getCatalogItem(String, String)} but only looking in legacy catalog
     * @deprecated since 0.12.0 only provided to allow TypeRegistry to see the legacy items */
    @Deprecated
    CatalogItem<?,?> getCatalogItemLegacy(String symbolicName, String version);

    /** @return Deletes the item with the given {@link CatalogItem#getSymbolicName()
     * symbolicName} and version
     * @throws NoSuchElementException if not found 
     * @deprecated since 0.12.0 use {@link BrooklynTypeRegistry} instead */
    @Deprecated
    void deleteCatalogItem(String symbolicName, String version);

    /** Deletes the item with the given {@link CatalogItem#getSymbolicName()
     * symbolicName} and version
     * @throws NoSuchElementException if not found 
     * @deprecated since introduced in 1.0.0, only used for transitioning */
    @Deprecated
    void deleteCatalogItem(String symbolicName, String version, boolean alsoCheckTypeRegistry, boolean failIfNotFound);
    
    /** variant of {@link #getCatalogItem(String, String)} which checks (and casts) type for convenience
     * (returns null if type does not match)
     * @deprecated since 0.12.0 use {@link BrooklynTypeRegistry} instead */ 
    @Deprecated
    <T,SpecT> CatalogItem<T,SpecT> getCatalogItem(Class<T> type, String symbolicName, String version);
    
    /** As non-legacy method but only looking in legacy catalog
     * @deprecated since 0.12.0 only provided to allow TypeRegistry to see the legacy items */
    @Deprecated
    <T,SpecT> CatalogItem<T,SpecT> getCatalogItemLegacy(Class<T> type, String symbolicName, String version);

    /** @return All items in the catalog
     * @deprecated since 0.12.0 use {@link BrooklynTypeRegistry} instead */ 
    @Deprecated
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems();

    /** As non-legacy method but only looking in legacy catalog
     * @deprecated since 0.12.0 only provided to allow TypeRegistry to see the legacy items */
    @Deprecated
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItemsLegacy();

    /** convenience for filtering items in the catalog; see CatalogPredicates for useful filters
     * @deprecated since 0.12.0 use {@link BrooklynTypeRegistry} instead */ 
    @Deprecated
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems(Predicate<? super CatalogItem<T,SpecT>> filter);

    /** As non-legacy method but only looking in legacy catalog
     * @deprecated since 0.12.0 only provided to allow TypeRegistry to see the legacy items */
    @Deprecated
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItemsLegacy(Predicate<? super CatalogItem<T,SpecT>> filter);

    /** persists the catalog item to the object store, if persistence is enabled */
    public void persist(CatalogItem<?, ?> catalogItem);

    /** @return The classloader which should be used to load classes and entities;
     * this includes all the catalog's classloaders in the right order.
     * This is a wrapper which will update as the underlying catalog items change,
     * so it is safe for callers to keep a handle on this. */
    public ClassLoader getRootClassLoader();

    /**
     * Creates a spec for the given catalog item, throwing exceptions if any problems.
     * 
     * Use of this method is strongly discouraged. It causes the catalog item to be re-parsed 
     * every time, which is very inefficient.
     * 
     * @deprecated since 0.10.0; use {@link #peekSpec(CatalogItem)} for a preview of what the item
     *             corresponds to.
     */
    @Deprecated
    <T, SpecT extends AbstractBrooklynObjectSpec<? extends T, SpecT>> SpecT createSpec(CatalogItem<T, SpecT> item);

    /** 
     * Creates a spec for the given catalog item, throwing exceptions if any problems. The returned 
     * spec is intended for getting additional information about the item. It should not be used 
     * for creating entities/apps!
     * 
     * See {@code EntityManagementUtils.createEntitySpecForApplication(ManagementContext mgmt, String plan)}
     * for how apps are deployed.
     * 
     * @since 0.10.0
     */
    AbstractBrooklynObjectSpec<?, ?> peekSpec(CatalogItem<?, ?> item);

    /** Adds the given registered types defined in a bundle's catalog BOM; 
     * no validation performed, so caller should do that subsequently after 
     * loading all potential inter-dependent types.
     * Optionally updates a supplied (empty) map containing newly added types as keys
     * and any previously existing type they replace as values, for reference or for use rolling back
     * (this is populated with work done so far if the method throws an error).
     * <p>
     * A null bundle can be supplied although that will cause oddness in production installations
     * that assume bundles for persistence, lookup and other capabilities. (But it's fine for tests.)
     * <p>
     * Most callers will want to use the OsgiManager to install a bundle. This is primarily to support that.
     */
    @Beta  // method may move elsewhere, or return type may change
    public void addTypesFromBundleBom(String yaml, @Nullable ManagedBundle bundle, boolean forceUpdate, Map<RegisteredType, RegisteredType> result);

    /** As {@link #addTypesFromBundleBom(String, ManagedBundle, boolean, Map)} followed by {@link #validateType(RegisteredType, RegisteredTypeLoadingContext, boolean)}
     * e.g. for use in tests and ad hoc setup when there is just a YAML file.  In OSGi mode this adds a bundle; otherwise it uses legacy catalog item addition mode.
     * <p>
     * Note that if addition or validation fails, this throws, unlike {@link #addTypesFromBundleBom(String, ManagedBundle, boolean, Map)},
     * and the type registry may have some of the items updated.  It is the caller's responsibility to clean up if required
     * (note, clean up is only possible if an empty results map is supplied to collect the items, and even then finding the bundle needs work;
     * or, most commonly, where the management context is just being thrown away, like in tests).
     * <p>
     * Most callers will want to use the OsgiManager to install a bundle. This is primarily to support that.
     */
    @Beta
    Collection<RegisteredType> addTypesAndValidateAllowInconsistent(String catalogYaml, @Nullable Map<RegisteredType, RegisteredType> result, boolean forceUpdate);

    /** As {@link #validateType(RegisteredType, RegisteredTypeLoadingContext, boolean)} allowing unresolved, taking a set of types,
     * and returning a map whose keys are those types where validation failed, mapped to the collection of errors validating that type.
     * An empty map result indicates no validation errors in the types passed in. 
     */
    @Beta  // method may move elsewhere
    public Map<RegisteredType,Collection<Throwable>> validateTypes(Iterable<RegisteredType> typesToValidate);

    /** Performs YAML validation on the given type, returning a collection of errors. 
     * An empty result indicates no validation errors in the type passed in. 
     * <p>
     * Validation may be side-effecting in that it sets metadata and refines supertypes
     * for the given registered type.
     */
    @Beta  // method may move elsewhere
    Collection<Throwable> validateType(RegisteredType typeToValidate, @Nullable RegisteredTypeLoadingContext optionalConstraint, boolean allowUnresolved);

    /** As {@link #addItems(String, boolean, boolean)}
     *
     * @deprecated since 1.1, use {@link #addTypesAndValidateAllowInconsistent(String, Map, boolean)} for direct access to catalog (non-OSGi, eg tests) or use the OsgiManager
     * (Used in tests.)
     */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml);

    /**
     * Adds items (represented in yaml) to the catalog.
     * (Only used for legacy-mode additions.) 
     * 
     * @param validate Whether to validate the types (default true)
     *
     * @param forceUpdate If true allows catalog update even when an
     * item exists with the same symbolicName and version
     *
     * @throws IllegalArgumentException if the yaml was invalid
     *
     * @deprecated Since 1.1, use {@link #addTypesFromBundleBom(String, ManagedBundle, boolean, Map)} for direct access to catalog (non/partial-OSGi, eg tests) or use the OsgiManager
     * (Used for REST API, a few tests, and legacy-compatibility additions.)
     */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml, boolean validate, boolean forceUpdate);

    /**
     * Adds items (represented in yaml) to the catalog coming from the indicated managed bundle.
     * Fails if the same version exists in catalog (unless snapshot).
     * (Only used for legacy-mode additions.)
     *
     * @param forceUpdate If true allows catalog update even when an
     * item exists with the same symbolicName and version
     *
     * @throws IllegalArgumentException if the yaml was invalid
     *
     * @deprecated Since 1.1, use {@link #addTypesFromBundleBom(String, ManagedBundle, boolean, Map)} for direct access to catalog (non/partial-OSGi, eg tests) or use the OsgiManager
     * (Only used for legacy OSGi bundles.) */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml, ManagedBundle bundle, boolean forceUpdate);
    
    /**
     * adds an item to the 'manual' catalog;
     * this does not update the classpath or have a record to the java Class
     *
     * @deprecated since 0.7.0 Construct catalogs with yaml (referencing OSGi bundles) instead
     */
    @Deprecated
    void addItem(CatalogItem<?,?> item);

    /**
     * adds the given items to the catalog, similar to {@link #reset(Collection)} but where it 
     * just adds without removing the existing content. Note this is very different from 
     * {@link #addItem(CatalogItem)}, which adds to the 'manual' catalog.
     *
     * @since 1.0.0 (only for legacy backwards compatibility)
     * @deprecated since 1.0.0; instead use bundles in persisted state!
     */
    @Deprecated
    void addCatalogLegacyItemsOnRebind(Iterable<? extends CatalogItem<?,?>> items);

    /**
     * Creates a catalog item and adds it to the 'manual' catalog,
     * with the corresponding Class definition (loaded by a classloader)
     * registered and available in the classloader.
     * <p>
     * Note that the class will be available for this session only,
     * although the record of the item will appear in the catalog DTO if exported,
     * so it is recommended to edit the 'manual' catalog DTO if using it to
     * generate a catalog, either adding the appropriate classpath URL or removing this entry.
     *
     * @deprecated since 0.7.0 Construct catalogs with OSGi bundles instead.
     * This is used in a handful of tests which should be rewritten to refer to OSGi bundles.
     */
    @Deprecated
    @VisibleForTesting
    CatalogItem<?,?> addItem(Class<?> clazz);

    /** @deprecated since 1.1 remove all bundles, that should clear the catalog
     * Used for legacy catalog initialization and rebind.
     */
    @Deprecated
    void reset(Collection<CatalogItem<?, ?>> entries);

}
