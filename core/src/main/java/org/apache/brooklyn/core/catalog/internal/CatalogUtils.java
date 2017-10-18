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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog.BrooklynLoaderTracker;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.classloading.BrooklynClassLoadingContextSequential;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl.RebindTracker;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypeNaming;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Time;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

public class CatalogUtils {
    private static final Logger log = LoggerFactory.getLogger(CatalogUtils.class);

    public static final char VERSION_DELIMITER = ':';

    public static BrooklynClassLoadingContext newClassLoadingContext(ManagementContext mgmt, CatalogItem<?, ?> item) {
        // TODO getLibraries() should never be null but sometimes it is still
        // e.g. run CatalogResourceTest without the above check
        if (item.getLibraries() == null) {
            log.debug("CatalogItemDtoAbstract.getLibraries() is null.", new Exception("Trace for null CatalogItemDtoAbstract.getLibraries()"));
        }
        return newClassLoadingContext(mgmt, item.getId(), item.getLibraries());
    }
    
    public static BrooklynClassLoadingContext newClassLoadingContext(ManagementContext mgmt, RegisteredType item) {
        return newClassLoadingContext(mgmt, item.getId(), item.getLibraries(), null);
    }
    
    /** made @Beta in 0.9.0 because we're not sure to what extent to support stacking loaders; 
     * only a couple places currently rely on such stacking, in general the item and the bundles *are* the context,
     * and life gets hard if we support complex stacking! */
    @Beta 
    public static BrooklynClassLoadingContext newClassLoadingContext(ManagementContext mgmt, RegisteredType item, BrooklynClassLoadingContext loader) {
        return newClassLoadingContext(mgmt, item.getId(), item.getLibraries(), loader);
    }
    
    public static BrooklynClassLoadingContext getClassLoadingContext(Entity entity) {
        ManagementContext mgmt = ((EntityInternal)entity).getManagementContext();
        String catId = entity.getCatalogItemId();
        if (Strings.isBlank(catId)) return JavaBrooklynClassLoadingContext.create(mgmt);
        Maybe<RegisteredType> cat = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(catId), RegisteredTypeLoadingContexts.spec(Entity.class));
        if (cat.isNull()) {
            log.warn("Cannot load "+catId+" to get classloader for "+entity+"; will try with standard loader, but might fail subsequently");
            return JavaBrooklynClassLoadingContext.create(mgmt);
        }
        return newClassLoadingContext(mgmt, cat.get(), JavaBrooklynClassLoadingContext.create(mgmt));
    }

    public static BrooklynClassLoadingContext newClassLoadingContext(@Nullable ManagementContext mgmt, String catalogItemId, Collection<? extends OsgiBundleWithUrl> libraries) {
        return newClassLoadingContext(mgmt, catalogItemId, libraries, JavaBrooklynClassLoadingContext.create(mgmt));
    }
    
    @Deprecated /** @deprecated since 0.9.0; we should now always have a registered type callers can pass instead of the catalog item id */
    private static BrooklynClassLoadingContext newClassLoadingContext(@Nullable ManagementContext mgmt, String catalogItemId, Collection<? extends OsgiBundleWithUrl> libraries, BrooklynClassLoadingContext loader) {
        BrooklynClassLoadingContextSequential result = new BrooklynClassLoadingContextSequential(mgmt);

        if (libraries!=null && !libraries.isEmpty()) {
            result.add(new OsgiBrooklynClassLoadingContext(mgmt, catalogItemId, libraries));
        }

        if (loader !=null) {
            // TODO determine whether to support stacking
            result.add(loader);
        }
        BrooklynClassLoadingContext threadLocalLoader = BrooklynLoaderTracker.getLoader();
        if (threadLocalLoader != null) {
            // TODO and determine if this is needed/wanted
            result.add(threadLocalLoader);
        }

        result.addSecondary(JavaBrooklynClassLoadingContext.create(mgmt));
        return result;
    }

    public static BrooklynClassLoadingContext newClassLoadingContextForCatalogItems(
        ManagementContext managementContext, String primaryItemId, List<String> searchPath) {

        BrooklynClassLoadingContextSequential seqLoader = new BrooklynClassLoadingContextSequential(managementContext);
        addSearchItem(managementContext, seqLoader, primaryItemId, false /* primary ID may be temporary */);
        for (String searchId : searchPath) {
            addSearchItem(managementContext, seqLoader, searchId, true);
        }
        return seqLoader;
    }

    /**
     * Registers all bundles with the management context's OSGi framework.
     */
    public static void installLibraries(ManagementContext managementContext, @Nullable Collection<CatalogBundle> libraries) {
        installLibraries(managementContext, libraries, true);
    }
    /** As {@link #installLibraries(ManagementContext, Collection)} but letting caller suppress the deferred start/install
     * (for use in tests where bundles' BOMs aren't resolvable). */
    public static void installLibraries(ManagementContext managementContext, @Nullable Collection<CatalogBundle> libraries, boolean startBundlesAndInstallToBrooklyn) {
        if (libraries == null) return;

        ManagementContextInternal mgmt = (ManagementContextInternal) managementContext;
        if (!libraries.isEmpty()) {
            Maybe<OsgiManager> osgi = mgmt.getOsgiManager();
            if (osgi.isAbsent()) {
                throw new IllegalStateException("Unable to load bundles "+libraries+" because OSGi is not running.");
            }
            if (log.isDebugEnabled()) 
                logDebugOrTraceIfRebinding(log, 
                    "Loading bundles in {}: {}", 
                    new Object[] {managementContext, Joiner.on(", ").join(libraries)});
            Stopwatch timer = Stopwatch.createStarted();
            List<OsgiBundleInstallationResult> results = MutableList.of();
            for (CatalogBundle bundleUrl : libraries) {
                OsgiBundleInstallationResult result = osgi.get().installDeferredStart(BasicManagedBundle.of(bundleUrl), null, true).get();
                if (log.isDebugEnabled()) {
                    logDebugOrTraceIfRebinding(log, "Installation of library "+bundleUrl+": "+result);
                }
                results.add(result);
            }
            Map<String, Throwable> errors = MutableMap.of();
            if (startBundlesAndInstallToBrooklyn) {
                for (OsgiBundleInstallationResult r: results) {
                    if (r.getDeferredStart()!=null) {
                        try {
                            r.getDeferredStart().run();
                        } catch (Throwable t) {
                            Exceptions.propagateIfFatal(t);
                            // above will done rollback for the failed item, but we need consistent behaviour for all libraries;
                            // for simplicity we simply have each bundle either fully installed or fully rolled back
                            // (alternative would be to roll back everything)
                            errors.put(r.getVersionedName().toString(), t);
                        }
                    }
                }
            }
            if (!errors.isEmpty()) {
                logDebugOrTraceIfRebinding(log, "Tried registering {} libraries, {} succeeded, but failed {} (throwing)", 
                    new Object[] { libraries.size(), libraries.size() - errors.size(), errors.keySet() });
                if (errors.size()==1) {
                    throw Exceptions.propagateAnnotated("Error starting referenced library in Brooklyn bundle "+Iterables.getOnlyElement(errors.keySet()), 
                        Iterables.getOnlyElement(errors.values()));
                } else {
                    throw Exceptions.create("Error starting referenced libraries in Brooklyn bundles "+errors.keySet(), errors.values());                    
                }
            }
            if (log.isDebugEnabled()) { 
                logDebugOrTraceIfRebinding(log, 
                    "Registered {} bundle{} in {}",
                    new Object[]{libraries.size(), Strings.s(libraries.size()), Time.makeTimeStringRounded(timer)});
            }
        }
    }

    /** Scans the given {@link BrooklynClassLoadingContext} to detect what catalog item id is in effect. */
    public static String getCatalogItemIdFromLoader(BrooklynClassLoadingContext loader) {
        if (loader instanceof OsgiBrooklynClassLoadingContext) {
            return ((OsgiBrooklynClassLoadingContext)loader).getCatalogItemId();
        } else {
            return null;
        }
    }

    // TODO should be addToCatalogSearchPathOnAddition
    public static void setCatalogItemIdOnAddition(Entity entity, BrooklynObject itemBeingAdded) {
        if (entity.getCatalogItemId()!=null) {
            if (itemBeingAdded.getCatalogItemId()==null) {
                if (log.isDebugEnabled())
                    BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
                        "Catalog item addition: "+entity+" from "+entity.getCatalogItemId()+" applying its catalog item ID to "+itemBeingAdded);
                final BrooklynObjectInternal addInternal = (BrooklynObjectInternal) itemBeingAdded;
                addInternal.setCatalogItemIdAndSearchPath(entity.getCatalogItemId(), entity.getCatalogItemIdSearchPath());
            } else {
                if (!itemBeingAdded.getCatalogItemId().equals(entity.getCatalogItemId())) {
                    // not a problem, but something to watch out for
                    log.debug("Cross-catalog item detected: "+entity+" from "+entity.getCatalogItemId()+" has "+itemBeingAdded+" from "+itemBeingAdded.getCatalogItemId());
                }
            }
        } else if (itemBeingAdded.getCatalogItemId()!=null) {
            if (log.isDebugEnabled())
                BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
                    "Catalog item addition: "+entity+" without catalog item ID has "+itemBeingAdded+" from "+itemBeingAdded.getCatalogItemId());
        }
    }

    @Beta
    public static void logDebugOrTraceIfRebinding(Logger log, String message, Object ...args) {
        if (RebindTracker.isRebinding())
            log.trace(message, args);
        else
            log.debug(message, args);
    }

    /** @deprecated since 0.12.0 - the "version starts with number" test this does is hokey; use 
     * either {@link RegisteredTypeNaming#isUsableTypeColonVersion(String)} for weak enforcement
     * or {@link RegisteredTypeNaming#isGoodTypeColonVersion(String)} for OSGi enforcement. */
    // several references, but all deprecated, most safe to remove after a cycle or two and bad verisons have been warned
    @Deprecated
    public static boolean looksLikeVersionedId(String versionedId) {
        if (versionedId==null) return false;
        int fi = versionedId.indexOf(VERSION_DELIMITER);
        if (fi<0) return false;
        int li = versionedId.lastIndexOf(VERSION_DELIMITER);
        if (li!=fi) {
            // if multiple colons, we say it isn't a versioned reference; the prefix in that case must understand any embedded versioning scheme
            // this fixes the case of:  http://localhost:8080
            return false;
        }
        String candidateVersion = versionedId.substring(li+1);
        if (!candidateVersion.matches("[0-9]+(|(\\.|_).*)")) {
            // version must start with a number, followed if by anything with full stop or underscore before any other characters
            // e.g.  foo:1  or foo:1.1  or foo:1_SNAPSHOT all supported, but not e.g. foo:bar (or chef:cookbook or docker:my/image)
            return false;
        }
        if (!RegisteredTypeNaming.isUsableTypeColonVersion(versionedId)) {
            // arguments that contain / or whitespace will pass here but calling code will likely be changed not to support it 
            log.warn("Reference '"+versionedId+"' is being treated as a versioned type but it "
                + "contains deprecated characters (slashes or whitespace); likely to be unsupported in future versions.");
        }
        return true;
    }

    public static String getSymbolicNameFromVersionedId(String versionedId) {
        if (versionedId == null) return null;
        int versionDelimiterPos = versionedId.lastIndexOf(VERSION_DELIMITER);
        if (versionDelimiterPos != -1) {
            return versionedId.substring(0, versionDelimiterPos);
        } else {
            return null;
        }
    }

    public static String getVersionFromVersionedId(String versionedId) {
        if (versionedId == null) return null;
        int versionDelimiterPos = versionedId.lastIndexOf(VERSION_DELIMITER);
        if (versionDelimiterPos != -1) {
            return versionedId.substring(versionDelimiterPos+1);
        } else {
            return null;
        }
    }

    public static String getVersionedId(String id, String version) {
        // TODO null checks
        return id + VERSION_DELIMITER + version;
    }

    /** @deprecated since 0.9.0 use {@link BrooklynTypeRegistry#get(String, org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind, Class)} */
    // only a handful of items remaining, requiring a CatalogItem:  two in REST, one in rebind, and one in ClassLoaderUtils
    @Deprecated
    public static CatalogItem<?, ?> getCatalogItemOptionalVersion(ManagementContext mgmt, String versionedId) {
        if (versionedId == null) return null;
        if (looksLikeVersionedId(versionedId)) {
            String id = getSymbolicNameFromVersionedId(versionedId);
            String version = getVersionFromVersionedId(versionedId);
            return mgmt.getCatalog().getCatalogItem(id, version);
        } else {
            return mgmt.getCatalog().getCatalogItem(versionedId, BrooklynCatalog.DEFAULT_VERSION);
        }
    }

    public static boolean isBestVersion(ManagementContext mgmt, CatalogItem<?,?> item) {
        RegisteredType best = RegisteredTypes.getBestVersion(mgmt.getTypeRegistry().getMatching(
            RegisteredTypePredicates.symbolicName(item.getSymbolicName())));
        if (best==null) return false;
        return (best.getVersion().equals(item.getVersion()));
    }

    /** @deprecated since it was introduced in 0.9.0; TBD where this should live */
    @Deprecated
    public static void setDeprecated(ManagementContext mgmt, String symbolicNameAndOptionalVersion, boolean newValue) {
        RegisteredType item = mgmt.getTypeRegistry().get(symbolicNameAndOptionalVersion);
        Preconditions.checkNotNull(item, "No such item: " + symbolicNameAndOptionalVersion);
        setDeprecated(mgmt, item.getSymbolicName(), item.getVersion(), newValue);
    }
    
    /** @deprecated since it was introduced in 0.9.0; TBD where this should live */
    @Deprecated
    public static void setDisabled(ManagementContext mgmt, String symbolicNameAndOptionalVersion, boolean newValue) {
        RegisteredType item = mgmt.getTypeRegistry().get(symbolicNameAndOptionalVersion);
        Preconditions.checkNotNull(item, "No such item: "+symbolicNameAndOptionalVersion);
        setDisabled(mgmt, item.getSymbolicName(), item.getVersion(), newValue);
    }
    
    /** @deprecated since it was introduced in 0.9.0; TBD where this should live */
    @Deprecated
    public static void setDeprecated(ManagementContext mgmt, String symbolicName, String version, boolean newValue) {
        CatalogItem<?, ?> item = mgmt.getCatalog().getCatalogItem(symbolicName, version);
        if (item!=null) {
            item.setDeprecated(newValue);
            mgmt.getCatalog().persist(item);
        } else {
            RegisteredType type = mgmt.getTypeRegistry().get(symbolicName, version);
            if (type!=null) {
                RegisteredTypes.setDeprecated(type, newValue);
            } else {
                throw new NoSuchElementException(symbolicName+":"+version);
            }
        }
    }

    /** @deprecated since it was introduced in 0.9.0; TBD where this should live */
    @Deprecated
    public static void setDisabled(ManagementContext mgmt, String symbolicName, String version, boolean newValue) {
        CatalogItem<?, ?> item = mgmt.getCatalog().getCatalogItem(symbolicName, version);
        if (item!=null) {
            item.setDisabled(newValue);
            mgmt.getCatalog().persist(item);
        } else {
            RegisteredType type = mgmt.getTypeRegistry().get(symbolicName, version);
            if (type!=null) {
                RegisteredTypes.setDisabled(type, newValue);
            } else {
                throw new NoSuchElementException(symbolicName+":"+version);
            }
        }
    }

    private static void addSearchItem(ManagementContext managementContext, BrooklynClassLoadingContextSequential loader, String itemId, boolean warnIfNotFound) {
        OsgiManager osgi = ((ManagementContextInternal)managementContext).getOsgiManager().orNull();
        boolean didSomething = false;
        if (osgi!=null) {
            ManagedBundle bundle = osgi.getManagedBundle(VersionedName.fromString(itemId));
            if (bundle!=null) {
                loader.add( newClassLoadingContext(managementContext, itemId, MutableSet.of(bundle)) );
                didSomething = true;
                // but also load entities, if name is same as a bundle and libraries are set on entity
            }
        }
        
        RegisteredType item = managementContext.getTypeRegistry().get(itemId);
        if (item != null) {
            BrooklynClassLoadingContext itemLoader = newClassLoadingContext(managementContext, item);
            loader.add(itemLoader);
            didSomething = true;
        }

        if (!didSomething) {
            if (warnIfNotFound) {
                log.warn("Can't find catalog item " + itemId+" when searching; a search path may be incomplete and other errors may follow");
            } else {
                log.trace("Can't find catalog item " + itemId+" when searching; ignoring as this can be normal in setup/scans, "
                    + "but it can also mean a search path may be incomplete and other errors may follow");
            }
        }
    }

    public static String[] bundleIds(Bundle bundle) {
        return new String[] {
            String.valueOf(bundle.getBundleId()), bundle.getSymbolicName(), bundle.getVersion().toString()
        };
    }
}
