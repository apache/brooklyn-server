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
package org.apache.brooklyn.core.mgmt.ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult.ResultCode;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.VersionComparator;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

// package-private so we can move this one if/when we move OsgiManager
class OsgiArchiveInstaller {

    private static final Logger log = LoggerFactory.getLogger(OsgiArchiveInstaller.class);
    
    final private OsgiManager osgiManager;
    private ManagedBundle suppliedKnownBundleMetadata;
    private InputStream zipIn;
    
    private boolean start = true;
    private boolean loadCatalogBom = true;
    private boolean force = false;
    private boolean deferredStart = false;
    private boolean validateTypes = true;
    
    private File zipFile;
    private Manifest discoveredManifest;
    private VersionedName discoveredBomVersionedName;
    OsgiBundleInstallationResult result;
    
    private ManagedBundle inferredMetadata;
    private final boolean inputStreamSupplied;
    
    OsgiArchiveInstaller(OsgiManager osgiManager, ManagedBundle knownBundleMetadata, InputStream zipIn) {
        this.osgiManager = osgiManager;
        this.suppliedKnownBundleMetadata = knownBundleMetadata;
        this.zipIn = zipIn;
        inputStreamSupplied = zipIn!=null;
    }

    public void setStart(boolean start) {
        this.start = start;
    }
    
    public void setLoadCatalogBom(boolean loadCatalogBom) {
        this.loadCatalogBom = loadCatalogBom;
    }
    
    public void setForce(boolean force) {
        this.force = force;
    }

    public void setDeferredStart(boolean deferredStart) {
        this.deferredStart = deferredStart;
    }
    
    public void setValidateTypes(boolean validateTypes) {
        this.validateTypes = validateTypes;
    }

    private ManagementContextInternal mgmt() {
        return (ManagementContextInternal) osgiManager.mgmt;
    }
    
    private synchronized void init() {
        if (result!=null) {
            if (zipFile!=null || zipIn==null) return;
            throw new IllegalStateException("This installer instance has already been used and the input stream discarded");
        }
        result = new OsgiBundleInstallationResult();
        inferredMetadata = suppliedKnownBundleMetadata==null ? new BasicManagedBundle() : suppliedKnownBundleMetadata;
    }
    
    private synchronized void makeLocalZipFileFromInputStreamOrUrl() {
        if (zipIn==null) {
            Maybe<Bundle> installedBundle = Maybe.absent();
            if (suppliedKnownBundleMetadata!=null) {
                // if no input stream, look for a URL and/or a matching bundle
                if (!suppliedKnownBundleMetadata.isNameResolved()) {
                    ManagedBundle mbFromUrl = osgiManager.getManagedBundleFromUrl(suppliedKnownBundleMetadata.getUrl());
                    if (mbFromUrl!=null) {
                        // user supplied just a URL (eg brooklyn.libraries), but we recognise it,
                        // so don't try to reload it, just record the info we know about it to retrieve the bundle
                        ((BasicManagedBundle)suppliedKnownBundleMetadata).setSymbolicName(mbFromUrl.getSymbolicName());
                        ((BasicManagedBundle)suppliedKnownBundleMetadata).setVersion(mbFromUrl.getSuppliedVersionString());
                    }
                }
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.getOsgiUniqueUrl()!=null) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).requiringFromUrl(suppliedKnownBundleMetadata.getOsgiUniqueUrl()).find();
                }
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.getUrl()!=null) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).requiringFromUrl(suppliedKnownBundleMetadata.getUrl()).find();
                }
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.isNameResolved()) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).symbolicName(suppliedKnownBundleMetadata.getSymbolicName()).version(suppliedKnownBundleMetadata.getSuppliedVersionString()).find();
                }
                if (suppliedKnownBundleMetadata.getUrl()!=null) {
                    if (installedBundle.isAbsent() || force) {
                        // reload 
                        zipIn = ResourceUtils.create(mgmt()).getResourceFromUrl(suppliedKnownBundleMetadata.getUrl());
                    }
                }
            }
            
            if (zipIn==null) {
                if (installedBundle.isPresent()) {
                    // no way to install (no url), or no need to install (not forced); just ignore it
                    result.metadata = osgiManager.getManagedBundle(new VersionedName(installedBundle.get()));
                    if (result.metadata==null) {
                        // low-level installed bundle
                        result.metadata = new BasicManagedBundle(installedBundle.get().getSymbolicName(), installedBundle.get().getVersion().toString(),
                            suppliedKnownBundleMetadata!=null ? suppliedKnownBundleMetadata.getUrl() : null);
                    }
                    result.setIgnoringAlreadyInstalled();
                    return;
                }
                result.metadata = suppliedKnownBundleMetadata;
                throw new IllegalArgumentException("No input stream available and no URL could be found; nothing to install");
            }
            result.bundle = installedBundle.orNull();
        }
        
        zipFile = Os.newTempFile("brooklyn-bundle-transient-"+suppliedKnownBundleMetadata, "zip");
        try (FileOutputStream fos = new FileOutputStream(zipFile)) {
            Streams.copy(zipIn, fos);
            zipIn.close();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            zipIn = null;
        }
    }

    private void discoverManifestFromCatalogBom(boolean isCatalogBomRequired) {
        discoveredManifest = new BundleMaker(mgmt()).getManifest(zipFile);
        ZipFile zf = null;
        try {
            try {
                zf = new ZipFile(zipFile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid ZIP/JAR archive: "+e);
            }
            ZipEntry bom = zf.getEntry(BasicBrooklynCatalog.CATALOG_BOM);
            if (bom==null) {
                bom = zf.getEntry("/"+BasicBrooklynCatalog.CATALOG_BOM);
            }
            if (bom==null) {
                if (isCatalogBomRequired) {
                    throw new IllegalArgumentException("Archive must contain a catalog.bom file in the root");
                } else {
                    return;
                }
            }
            String bomS;
            try {
                bomS = Streams.readFullyString(zf.getInputStream(bom));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error reading catalog.bom from ZIP/JAR archive: "+e);
            }
            discoveredBomVersionedName = BasicBrooklynCatalog.getVersionedName( BasicBrooklynCatalog.getCatalogMetadata(bomS), false );
        } finally {
            Streams.closeQuietly(zf);
        }
    }
    
    private void updateManifestFromAllSourceInformation() {
        if (discoveredBomVersionedName!=null) {
            matchSetOrFail("catalog.bom in archive", discoveredBomVersionedName.getSymbolicName(), discoveredBomVersionedName.getVersionString());
        }
        
        boolean manifestNeedsUpdating = false;
        if (discoveredManifest==null) {
            discoveredManifest = new Manifest();
            manifestNeedsUpdating = true;
        }
        if (!matchSetOrFail("MANIFEST.MF in archive", discoveredManifest.getMainAttributes().getValue(Constants.BUNDLE_SYMBOLICNAME),
                discoveredManifest.getMainAttributes().getValue(Constants.BUNDLE_VERSION))) {
            manifestNeedsUpdating = true;                
            discoveredManifest.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, inferredMetadata.getSymbolicName());
            discoveredManifest.getMainAttributes().putValue(Constants.BUNDLE_VERSION, inferredMetadata.getOsgiVersionString());
        }
        if (Strings.isBlank(inferredMetadata.getSymbolicName())) {
            throw new IllegalArgumentException("Missing bundle symbolic name in BOM or MANIFEST");
        }
        if (Strings.isBlank(inferredMetadata.getSuppliedVersionString())) {
            throw new IllegalArgumentException("Missing bundle version in BOM or MANIFEST");
        }
        if (discoveredManifest.getMainAttributes().getValue(Attributes.Name.MANIFEST_VERSION)==null) {
            discoveredManifest.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), BasicBrooklynCatalog.OSGI_MANIFEST_VERSION_VALUE);
            manifestNeedsUpdating = true;                
        }
        if (manifestNeedsUpdating) {
            File zf2 = new BundleMaker(mgmt()).copyAddingManifest(zipFile, discoveredManifest);
            zipFile.delete();
            zipFile = zf2;
        }
    }
    
    private synchronized void close() {
        if (zipFile!=null) {
            zipFile.delete();
            zipFile = null;
        }
    }
    
    /**
     * Installs a bundle, taking from ZIP input stream if supplied, falling back to URL in the {@link ManagedBundle} metadata supplied.
     * It will take metadata from any of: a MANIFEST.MF in the ZIP; a catalog.bom in the ZIP; the {@link ManagedBundle} metadata supplied.
     * If metadata is supplied in multiple such places, it must match.
     * Appropriate metadata will be added to the ZIP and installation attempted.
     * <p>
     * If a matching bundle is already installed, the installation will stop with a {@link ResultCode#IGNORING_BUNDLE_AREADY_INSTALLED}
     * unless the bundle is a snapshot or "force" is specified.
     * In the latter two cases, if there is an installed matching bundle, that bundle will be updated with the input stream here,
     * with any catalog items from the old bundle removed and those from this bundle installed.
     * <p>
     * Default behaviour is {@link #setLoadCatalogBom(boolean)} true and {@link #setForce(boolean)} false.
     * <p>
     * The return value is extensive but should be self-evident, and will include a list of any registered types (catalog items) installed. 
     */
    public ReferenceWithError<OsgiBundleInstallationResult> install() {
        boolean startedInstallation = false;
        
        try {
            init();
            makeLocalZipFileFromInputStreamOrUrl();
            if (result.code!=null) return ReferenceWithError.newInstanceWithoutError(result);
            discoverManifestFromCatalogBom(false);
            if (result.code!=null) return ReferenceWithError.newInstanceWithoutError(result);
            updateManifestFromAllSourceInformation();
            if (result.code!=null) return ReferenceWithError.newInstanceWithoutError(result);
            assert inferredMetadata.isNameResolved() : "Should have resolved "+inferredMetadata;
            assert inferredMetadata instanceof BasicManagedBundle : "Only BasicManagedBundles supported";
            ((BasicManagedBundle)inferredMetadata).setChecksum(getChecksum(new ZipFile(zipFile)));

            final boolean updating;
            result.metadata = osgiManager.getManagedBundle(inferredMetadata.getVersionedName());
            if (result.getMetadata()!=null) {
                // already have a managed bundle - check if this is using a new/different URL
                if (suppliedKnownBundleMetadata!=null && suppliedKnownBundleMetadata.getUrl()!=null) {
                    String knownIdForThisUrl = osgiManager.managedBundlesRecord.getManagedBundleIdFromUrl(suppliedKnownBundleMetadata.getUrl());
                    if (knownIdForThisUrl==null) {
                        // it's a new URL, but a bundle we already know about
                        log.warn("Request to install from "+suppliedKnownBundleMetadata.getUrl()+" which is not recognized but "+
                            "appears to match "+result.getMetadata()+"; now associating with the latter");
                        osgiManager.managedBundlesRecord.setManagedBundleUrl(suppliedKnownBundleMetadata.getUrl(), result.getMetadata().getId());
                    } else if (!knownIdForThisUrl.equals(result.getMetadata().getId())) {
                        log.warn("Request to install from "+suppliedKnownBundleMetadata.getUrl()+" which is associated to "+knownIdForThisUrl+" but "+
                            "appears to match "+result.getMetadata()+"; now associating with the latter");
                        osgiManager.managedBundlesRecord.setManagedBundleUrl(suppliedKnownBundleMetadata.getUrl(), result.getMetadata().getId());
                    }
                }
                if (canUpdate()) { 
                    result.bundle = osgiManager.framework.getBundleContext().getBundle(result.getMetadata().getOsgiUniqueUrl());
                    if (result.getBundle()==null) {
                        log.warn("Brooklyn thought is was already managing bundle "+result.getMetadata().getVersionedName()+" but it's not installed to framework; reinstalling it");
                        updating = false;
                    } else {
                        updating = true;
                    }
                } else {
                    if (result.getMetadata().getChecksum()==null || inferredMetadata.getChecksum()==null) {
                        log.warn("Missing bundle checksum data for "+result+"; assuming bundle replacement is permitted");
                    } else if (!Objects.equal(result.getMetadata().getChecksum(), inferredMetadata.getChecksum())) {
                        throw new IllegalArgumentException("Bundle "+result.getMetadata().getVersionedName()+" already installed; "
                            + "cannot install a different bundle at a same non-snapshot version");
                    }
                    result.setIgnoringAlreadyInstalled();
                    return ReferenceWithError.newInstanceWithoutError(result);
                }
            } else {
                result.metadata = inferredMetadata;
                // no such managed bundle
                Maybe<Bundle> b = Osgis.bundleFinder(osgiManager.framework).symbolicName(result.getMetadata().getSymbolicName()).version(result.getMetadata().getSuppliedVersionString()).find();
                if (b.isPresent()) {
                    // bundle already installed to OSGi subsystem but brooklyn not aware of it;
                    // this will often happen on a karaf restart so don't be too strict!
                    // in this case let's uninstall it to make sure we have the right bundle and checksum
                    // (in case where user has replaced a JAR file in persisted state,
                    // or where they osgi installed something and are now uploading it or something else) 
                    // but let's just assume it's the same; worst case if not user will
                    // have to uninstall it then reinstall it to do the replacement
                    // (means you can't just replace a JAR in persisted state however)
                    log.debug("Brooklyn install of "+result.getMetadata().getVersionedName()+" detected already loaded in OSGi; uninstalling that to reinstall as Brooklyn-managed");
                    b.get().uninstall();
                }
                // normal install
                updating = false;
            }
            
            startedInstallation = true;
            try (InputStream fin = new FileInputStream(zipFile)) {
                if (!updating) {
                    assert result.getBundle()==null;
                    result.bundle = osgiManager.framework.getBundleContext().installBundle(result.getMetadata().getOsgiUniqueUrl(), fin);
                } else {
                    result.bundle.update(fin);
                }
            }
            
            osgiManager.checkCorrectlyInstalled(result.getMetadata(), result.bundle);
            final File oldZipFile; 
            
            if (!updating) { 
                oldZipFile = null;
                osgiManager.managedBundlesRecord.addManagedBundle(result, zipFile);
                result.code = OsgiBundleInstallationResult.ResultCode.INSTALLED_NEW_BUNDLE;
                result.message = "Installed Brooklyn catalog bundle "+result.getMetadata().getVersionedName()+" with ID "+result.getMetadata().getId()+" ["+result.bundle.getBundleId()+"]";
                ((BasicManagedBundle)result.getMetadata()).setPersistenceNeeded(true);
                mgmt().getRebindManager().getChangeListener().onManaged(result.getMetadata());
            } else {
                oldZipFile = osgiManager.managedBundlesRecord.updateManagedBundleFile(result, zipFile);
                result.code = OsgiBundleInstallationResult.ResultCode.UPDATED_EXISTING_BUNDLE;
                result.message = "Updated Brooklyn catalog bundle "+result.getMetadata().getVersionedName()+" as existing ID "+result.getMetadata().getId()+" ["+result.bundle.getBundleId()+"]";
                ((BasicManagedBundle)result.getMetadata()).setPersistenceNeeded(true);
                mgmt().getRebindManager().getChangeListener().onChanged(result.getMetadata());
            }
            log.debug(result.message + " (in osgi container)");
            // can now delete and close (copy has been made and is available from OsgiManager)
            zipFile.delete();
            zipFile = null;
            
            // setting the above before the code below means if there is a problem starting or loading catalog items
            // a user has to remove then add again, or forcibly reinstall;
            // that seems fine and probably better than allowing bundles to start and catalog items to be installed 
            // when brooklyn isn't aware it is supposed to be managing it
            
            // starting here flags wiring issues earlier
            // but may break some things running from the IDE
            // eg if it doesn't have OSGi deps, or if it doesn't have camp parser,
            // or if caller is installing multiple things that depend on each other
            // eg rebind code, brooklyn.libraries list -- deferred start allows caller to
            // determine whether not to start or to start all after things are installed
            Runnable startRunnable = new Runnable() {
                private void rollbackBundle() {
                    if (updating) {
                        if (oldZipFile==null) {
                            throw new IllegalStateException("Did not have old ZIP file to install");
                        }
                        log.debug("Rolling back bundle "+result.getVersionedName()+" to state from "+oldZipFile);
                        try {
                            File zipFileNow = osgiManager.managedBundlesRecord.rollbackManagedBundleFile(result, oldZipFile);
                            result.bundle.update(new FileInputStream(Preconditions.checkNotNull(zipFileNow, "Couldn't find contents of old version of bundle")));
                        } catch (Exception e) {
                            Exceptions.propagateIfFatal(e);
                            log.error("Error rolling back following failed install of updated "+result.getVersionedName()+"; "
                                + "installation will likely be corrupted and correct version should be manually installed.", e);
                        }
                        
                        ((BasicManagedBundle)result.getMetadata()).setPersistenceNeeded(true);
                        mgmt().getRebindManager().getChangeListener().onChanged(result.getMetadata());
                    } else {
                        log.debug("Uninstalling bundle "+result.getVersionedName()+" (roll back of failed fresh install, no previous version to revert to)");
                        osgiManager.uninstallUploadedBundle(result.getMetadata());
                        
                        ((BasicManagedBundle)result.getMetadata()).setPersistenceNeeded(true);
                        mgmt().getRebindManager().getChangeListener().onUnmanaged(result.getMetadata());
                    }
                }
                public void run() {
                    if (start) {
                        try {
                            log.debug("Starting bundle "+result.getVersionedName());
                            result.bundle.start();
                        } catch (BundleException e) {
                            log.warn("Error starting bundle "+result.getVersionedName()+", uninstalling, restoring any old bundle, then re-throwing error: "+e);
                            try {
                                rollbackBundle();
                            } catch (Throwable t) {
                                Exceptions.propagateIfFatal(t);
                                log.warn("Error rolling back "+result.getVersionedName()+" after bundle start problem; server may be in inconsistent state (swallowing this error and propagating installation error): "+Exceptions.collapseText(t), t);
                                throw Exceptions.propagate(new BundleException("Failure installing and rolling back; server may be in inconsistent state regarding bundle "+result.getVersionedName()+". "
                                    + "Rollback failure ("+Exceptions.collapseText(t)+") detailed in log. Installation error is: "+Exceptions.collapseText(e), e));
                            }
                            
                            throw Exceptions.propagate(e);
                        }
                    }
        
                    if (loadCatalogBom) {
                        Iterable<RegisteredType> itemsFromOldBundle = null;
                        Map<RegisteredType, RegisteredType> itemsReplacedHere = null;
                        try {
                            if (updating) {
                                itemsFromOldBundle = osgiManager.uninstallCatalogItemsFromBundle( result.getVersionedName() );
                                // (ideally removal and addition would be atomic)
                            }
                            itemsReplacedHere = MutableMap.of();
                            osgiManager.loadCatalogBom(result.bundle, force, validateTypes, itemsReplacedHere);
                            Iterable<RegisteredType> items = mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(result.getMetadata()));
                            log.debug("Adding items from bundle "+result.getVersionedName()+": "+items);
                            for (RegisteredType ci: items) {
                                result.addType(ci);
                            }
                        } catch (Exception e) {
                            // unable to install new items; rollback bundles
                            // and reload replaced items
                            log.warn("Error adding Brooklyn items from bundle "+result.getVersionedName()+", uninstalling, restoring any old bundle and items, then re-throwing error: "+Exceptions.collapseText(e));
                            try {
                                rollbackBundle();
                            } catch (Throwable t) {
                                Exceptions.propagateIfFatal(t);
                                log.warn("Error rolling back "+result.getVersionedName()+" after catalog install problem; server may be in inconsistent state (swallowing this error and propagating installation error): "+Exceptions.collapseText(t), t);
                                throw Exceptions.propagate(new BundleException("Failure loading catalog items, and also failed rolling back; server may be in inconsistent state regarding bundle "+result.getVersionedName()+". "
                                    + "Rollback failure ("+Exceptions.collapseText(t)+") detailed in log. Installation error is: "+Exceptions.collapseText(e), e));
                            }
                            if (itemsFromOldBundle!=null) {
                                // add back all itemsFromOldBundle (when replacing a bundle)
                                for (RegisteredType oldItem: itemsFromOldBundle) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("RESTORING replaced bundle item "+oldItem+"\n"+RegisteredTypes.getImplementationDataStringForSpec(oldItem));
                                    }
                                    ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(oldItem, true);
                                }
                            }
                            if (itemsReplacedHere!=null) {
                                // and restore any items from other bundles (eg wrappers) that were replaced
                                MutableList<RegisteredType> replaced = MutableList.copyOf(itemsReplacedHere.values());
                                // in reverse order so if other bundle adds multiple we end up with the real original
                                Collections.reverse(replaced);
                                for (RegisteredType oldItem: replaced) {
                                    if (oldItem!=null) {
                                        if (log.isTraceEnabled()) {
                                            log.trace("RESTORING replaced external item "+oldItem+"\n"+RegisteredTypes.getImplementationDataStringForSpec(oldItem));
                                        }
                                        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(oldItem, true);
                                    }
                                }
                            }
                            
                            throw Exceptions.propagate(e);
                        }
                    }
                }
            };
            if (deferredStart) {
                result.deferredStart = startRunnable;
                log.debug(result.message+" (Brooklyn load deferred)");
            } else {
                startRunnable.run();
                if (!result.typesInstalled.isEmpty()) {
                    // show fewer info messages, only for 'interesting' and non-deferred installations
                    // (rebind is deferred, as are tests, but REST is not)
                    final int MAX_TO_LIST_EXPLICITLY = 5;
                    Iterable<String> firstN = Iterables.transform(MutableList.copyOf(Iterables.limit(result.typesInstalled, MAX_TO_LIST_EXPLICITLY)),
                        new Function<RegisteredType,String>() {
                            @Override public String apply(RegisteredType input) {
                                return input.getVersionedName().toString();
                            }
                        });
                    log.info(result.message+", items: "+firstN+
                        (result.typesInstalled.size() > MAX_TO_LIST_EXPLICITLY ? " (and others, "+result.typesInstalled.size()+" total)" : "") );
                    if (log.isDebugEnabled() && result.typesInstalled.size()>MAX_TO_LIST_EXPLICITLY) {
                        log.debug(result.message+", all items: "+result.typesInstalled);
                    }
                } else {
                    log.debug(result.message+" (into Brooklyn), with no catalog items");
                }
            }

            return ReferenceWithError.newInstanceWithoutError(result);
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            result.code = startedInstallation ? OsgiBundleInstallationResult.ResultCode.ERROR_LAUNCHING_BUNDLE : OsgiBundleInstallationResult.ResultCode.ERROR_PREPARING_BUNDLE;
            result.message = "Bundle "+inferredMetadata+" failed "+
                (startedInstallation ? "installation" : "preparation") + ": " + Exceptions.collapseText(e);
            return ReferenceWithError.newInstanceThrowingError(result, new IllegalStateException(result.message, e));
        } finally {
            close();
        }
    }

    private static String getChecksum(ZipFile zf) {
        // checksum should ignore time/date stamps on files - just look at entries and contents. also ignore order.
        // (tests fail without time/date is one reason, but really if a person rebuilds a ZIP that is the same 
        // files we should treat it as identical)
        try {
            Map<String,String> entriesToChecksum = MutableMap.of();
            for (ZipEntry ze: Collections.list(zf.entries())) {
                entriesToChecksum.put(ze.getName(), Streams.getMd5Checksum(zf.getInputStream(ze)));
            }
            return Streams.getMd5Checksum(Streams.newInputStreamWithContents(new TreeMap<>(entriesToChecksum).toString()));
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    private boolean canUpdate() {
        // only update if forced, or it's a snapshot for which a byte stream is supplied
        // (IE don't update a snapshot verison every time its URL is referenced in a 'libraries' section)
        return force || (VersionComparator.isSnapshot(inferredMetadata.getSuppliedVersionString()) && inputStreamSupplied);
    }

    /** true if the supplied name and version are complete; updates if the known data is incomplete;
     * throws if there is a mismatch; false if the supplied data is incomplete */
    private boolean matchSetOrFail(String source, String name, String version) {
        boolean suppliedIsComplete = true;
        if (Strings.isBlank(name)) {
            suppliedIsComplete = false;
        } else if (Strings.isBlank(inferredMetadata.getSymbolicName())) {
            ((BasicManagedBundle)inferredMetadata).setSymbolicName(name);
        } else if (!Objects.equal(inferredMetadata.getSymbolicName(), name)){
            throw new IllegalArgumentException("Symbolic name mismatch '"+name+"' from "+source+" (expected '"+inferredMetadata.getSymbolicName()+"')");
        }
        
        if (Strings.isBlank(version)) {
            suppliedIsComplete = false;
        } else if (Strings.isBlank(inferredMetadata.getSuppliedVersionString())) {
            ((BasicManagedBundle)inferredMetadata).setVersion(version);
        } else if (!BrooklynVersionSyntax.equalAsOsgiVersions(inferredMetadata.getSuppliedVersionString(), version)) {
            throw new IllegalArgumentException("Bundle version mismatch '"+version+"' from "+source+" (expected '"+inferredMetadata.getSuppliedVersionString()+"')");
        }
        
        return suppliedIsComplete;
    }

}
