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
     * returning null if no matches are found. */
    CatalogItem<?,?> getCatalogItem(String symbolicName, String version);

    /** @return Deletes the item with the given {@link CatalogItem#getSymbolicName()
     * symbolicName} and version
     * @throws NoSuchElementException if not found */
    void deleteCatalogItem(String symbolicName, String version);

    /** variant of {@link #getCatalogItem(String, String)} which checks (and casts) type for convenience
     * (returns null if type does not match) */
    <T,SpecT> CatalogItem<T,SpecT> getCatalogItem(Class<T> type, String symbolicName, String version);

    /** @return All items in the catalog */
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems();

    /** convenience for filtering items in the catalog; see CatalogPredicates for useful filters */
    <T,SpecT> Iterable<CatalogItem<T,SpecT>> getCatalogItems(Predicate<? super CatalogItem<T,SpecT>> filter);

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
     */
    @Beta  // method may move elsewhere, or return type may change
    public void addTypesFromBundleBom(String yaml, ManagedBundle bundle, boolean forceUpdate, Map<RegisteredType, RegisteredType> result);
    
    /** As {@link #validateType(RegisteredType)} but taking a set of types, returning a map whose keys are
     * those types where validation failed, mapped to the collection of errors validating that type. 
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
    Collection<Throwable> validateType(RegisteredType typeToValidate, @Nullable RegisteredTypeLoadingContext optionalConstraint);


    /**
     * Adds an item (represented in yaml) to the catalog.
     * Fails if the same version exists in catalog.
     *
     * @throws IllegalArgumentException if the yaml was invalid
     * @deprecated since 0.7.0 use {@link #addItems(String, boolean)}
     */
    @Deprecated
    CatalogItem<?,?> addItem(String yaml);
    
    /**
     * Adds an item (represented in yaml) to the catalog.
     * 
     * @param forceUpdate If true allows catalog update even when an
     * item exists with the same symbolicName and version
     *
     * @throws IllegalArgumentException if the yaml was invalid
     * @deprecated since 0.7.0 use {@link #addItems(String, boolean)}
     */
    @Deprecated
    CatalogItem<?,?> addItem(String yaml, boolean forceUpdate);
    
    /**
     * As {@link #addItemsFromBundle(String, ManagedBundle)} with a null bundle.
     */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml);
    
    /**
     * Adds items (represented in yaml) to the catalog coming from the indicated managed bundle.
     * Fails if the same version exists in catalog (unless snapshot).
     *
     * @throws IllegalArgumentException if the yaml was invalid
     */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml, @Nullable ManagedBundle definingBundle);
    
    /**
     * Adds items (represented in yaml) to the catalog.
     * 
     * @param forceUpdate If true allows catalog update even when an
     * item exists with the same symbolicName and version
     *
     * @throws IllegalArgumentException if the yaml was invalid
     */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml, boolean forceUpdate);
    
    /** As {@link #addItems(String, ManagedBundle)} but exposing forcing option as per {@link #addItem(String, boolean)}. */
    Iterable<? extends CatalogItem<?,?>> addItems(String yaml, ManagedBundle bundle, boolean forceUpdate);
    
    /**
     * adds an item to the 'manual' catalog;
     * this does not update the classpath or have a record to the java Class
     *
     * @deprecated since 0.7.0 Construct catalogs with yaml (referencing OSGi bundles) instead
     */
    // TODO maybe this should stay on the API? -AH Apr 2015 
    @Deprecated
    void addItem(CatalogItem<?,?> item);

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

    void reset(Collection<CatalogItem<?, ?>> entries);

}
