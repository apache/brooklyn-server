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
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult.ResultCode;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.VersionComparator;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;

import com.google.common.base.Objects;

// package-private so we can move this one if/when we move OsgiManager
class OsgiArchiveInstaller {

    final private OsgiManager osgiManager;
    private ManagedBundle suppliedKnownBundleMetadata;
    private InputStream zipIn;
    
    private boolean loadCatalogBom;
    private boolean force;
    
    private File zipFile;
    private Manifest discoveredManifest;
    private VersionedName discoveredBomVersionedName;
    OsgiBundleInstallationResult result;
    
    private ManagedBundle inferredMetadata;
    
    OsgiArchiveInstaller(OsgiManager osgiManager, ManagedBundle knownBundleMetadata, InputStream zipIn) {
        this.osgiManager = osgiManager;
        this.suppliedKnownBundleMetadata = knownBundleMetadata;
        this.zipIn = zipIn;
    }

    public void setLoadCatalogBom(boolean loadCatalogBom) {
        this.loadCatalogBom = loadCatalogBom;
    }
    
    public void setForce(boolean force) {
        this.force = force;
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
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.getOsgiUniqueUrl()!=null) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).requiringFromUrl(suppliedKnownBundleMetadata.getOsgiUniqueUrl()).find();
                }
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.getUrl()!=null) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).requiringFromUrl(suppliedKnownBundleMetadata.getUrl()).find();
                }
                if (installedBundle.isAbsent() && suppliedKnownBundleMetadata.isNameResolved()) {
                    installedBundle = Osgis.bundleFinder(osgiManager.framework).symbolicName(suppliedKnownBundleMetadata.getSymbolicName()).version(suppliedKnownBundleMetadata.getVersion()).find();
                }
                if (suppliedKnownBundleMetadata.getUrl()!=null) {
                    if (installedBundle.isAbsent() || force) {
                        // reload 
                        zipIn = ResourceUtils.create(mgmt()).getResourceFromUrl(suppliedKnownBundleMetadata.getUrl());
                    }
                }
            }
            
            if (installedBundle.isPresent()) {
                result.bundle = installedBundle.get();
                
                if (zipIn==null) {
                    // no way to install (no url), or no need to install (not forced); just ignore it
                    result.metadata = osgiManager.getManagedBundle(new VersionedName(installedBundle.get()));
                    result.setIgnoringAlreadyInstalled();
                    return;
                }
            } else {
                result.metadata = suppliedKnownBundleMetadata;
                throw new IllegalArgumentException("No input stream available and no URL could be found; nothing to install");
            }
        }
        
        zipFile = Os.newTempFile("brooklyn-bundle-transient-"+suppliedKnownBundleMetadata, "zip");
        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            Streams.copy(zipIn, fos);
            zipIn.close();
            fos.close();
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
            ZipEntry bom = zf.getEntry("catalog.bom");
            if (bom==null) {
                bom = zf.getEntry("/catalog.bom");
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
            matchSetOrFail("catalog.bom in archive", discoveredBomVersionedName.getSymbolicName(), discoveredBomVersionedName.getVersion().toString());
        }
        
        boolean manifestNeedsUpdating = false;
        if (discoveredManifest==null) {
            discoveredManifest = new Manifest();
            manifestNeedsUpdating = true;
        }
        if (!matchSetOrFail("MANIFEST.MF in archive", discoveredManifest.getMainAttributes().getValue(Constants.BUNDLE_SYMBOLICNAME),
                discoveredManifest.getMainAttributes().getValue(Constants.BUNDLE_VERSION) )) {
            manifestNeedsUpdating = true;                
            discoveredManifest.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, inferredMetadata.getSymbolicName());
            discoveredManifest.getMainAttributes().putValue(Constants.BUNDLE_VERSION, inferredMetadata.getVersion());
        }
        if (Strings.isBlank(inferredMetadata.getSymbolicName())) {
            throw new IllegalArgumentException("Missing bundle symbolic name in BOM or MANIFEST");
        }
        if (Strings.isBlank(inferredMetadata.getVersion())) {
            throw new IllegalArgumentException("Missing bundle version in BOM or MANIFEST");
        }
        if (discoveredManifest.getMainAttributes().getValue(Attributes.Name.MANIFEST_VERSION)==null) {
            discoveredManifest.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
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

            Boolean updating = null;
            result.metadata = osgiManager.getManagedBundle(inferredMetadata.getVersionedName());
            if (result.getMetadata()!=null) {
                // already have a managed bundle
                if (canUpdate()) { 
                    result.bundle = osgiManager.framework.getBundleContext().getBundle(result.getMetadata().getOsgiUniqueUrl());
                    if (result.getBundle()==null) {
                        throw new IllegalStateException("Detected already managing bundle "+result.getMetadata().getVersionedName()+" but framework cannot find it");
                    }
                    updating = true;
                } else {
                    result.setIgnoringAlreadyInstalled();
                    return ReferenceWithError.newInstanceWithoutError(result);
                }
            } else {
                result.metadata = inferredMetadata;
                // no such managed bundle
                Maybe<Bundle> b = Osgis.bundleFinder(osgiManager.framework).symbolicName(result.getMetadata().getSymbolicName()).version(result.getMetadata().getVersion()).find();
                if (b.isPresent()) {
                    // if it's non-brooklyn installed then fail
                    // (e.g. someone trying to install brooklyn or guice through this mechanism!)
                    result.bundle = b.get();
                    result.code = OsgiBundleInstallationResult.ResultCode.ERROR_INSTALLING_BUNDLE;
                    throw new IllegalStateException("Bundle "+result.getMetadata().getVersionedName()+" already installed in framework but not managed by Brooklyn; cannot install or update through Brooklyn");
                }
                // normal install
                updating = false;
            }
            
            startedInstallation = true;
            try (InputStream fin = new FileInputStream(zipFile)) {
                if (!updating) {
                    // install new
                    assert result.getBundle()==null;
                    result.bundle = osgiManager.framework.getBundleContext().installBundle(result.getMetadata().getOsgiUniqueUrl(), fin);
                } else {
                    result.bundle.update(fin);
                }
            }
            
            osgiManager.checkCorrectlyInstalled(result.getMetadata(), result.bundle);
            ((BasicManagedBundle)result.getMetadata()).setTempLocalFileWhenJustUploaded(zipFile);
            zipFile = null; // don't close/delete it here, we'll use it for uploading, then it will delete it
            
            if (!updating) { 
                synchronized (osgiManager.managedBundles) {
                    osgiManager.managedBundles.put(result.getMetadata().getId(), result.getMetadata());
                    osgiManager.managedBundleIds.put(result.getMetadata().getVersionedName(), result.getMetadata().getId());
                }
                result.code = OsgiBundleInstallationResult.ResultCode.INSTALLED_NEW_BUNDLE;
                result.message = "Installed "+result.getMetadata().getVersionedName()+" with ID "+result.getMetadata().getId();
                mgmt().getRebindManager().getChangeListener().onManaged(result.getMetadata());
            } else {
                result.code = OsgiBundleInstallationResult.ResultCode.UPDATED_EXISTING_BUNDLE;
                result.message = "Updated "+result.getMetadata().getVersionedName()+" as existing ID "+result.getMetadata().getId();
                mgmt().getRebindManager().getChangeListener().onChanged(result.getMetadata());
            }
            // setting the above before the code below means if there is a problem starting or loading catalog items
            // a user has to remove then add again, or forcibly reinstall;
            // that seems fine and probably better than allowing bundles to start and catalog items to be installed 
            // when brooklyn isn't aware it is supposed to be managing it
            
            // starting here  flags wiring issues earlier
            // but may break some things running from the IDE
            result.bundle.start();

            if (updating!=null) {
                osgiManager.uninstallCatalogItemsFromBundle( result.getVersionedName() );
                // (ideally removal and addition would be atomic)
            }
            if (loadCatalogBom) {
                osgiManager.loadCatalogBom(result.bundle);
            }

            return ReferenceWithError.newInstanceWithoutError(result);
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            result.code = startedInstallation ? OsgiBundleInstallationResult.ResultCode.ERROR_INSTALLING_BUNDLE : OsgiBundleInstallationResult.ResultCode.ERROR_PREPARING_BUNDLE;
            result.message = "Bundle "+inferredMetadata+" failed "+
                (startedInstallation ? "installation" : "preparation") + ": " + Exceptions.collapseText(e);
            return ReferenceWithError.newInstanceThrowingError(result, new IllegalStateException(result.message, e));
        } finally {
            close();
        }
    }

    private boolean canUpdate() {
        return force || VersionComparator.isSnapshot(inferredMetadata.getVersion());
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
        } else if (Strings.isBlank(inferredMetadata.getVersion())) {
            ((BasicManagedBundle)inferredMetadata).setVersion(version);
        } else if (!Objects.equal(inferredMetadata.getVersion(), version)){
            throw new IllegalArgumentException("Bundle version mismatch '"+version+"' from "+source+" (expected '"+inferredMetadata.getVersion()+"')");
        }
        
        return suppliedIsComplete;
    }    
}
