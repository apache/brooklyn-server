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
package org.apache.brooklyn.util.core.osgi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.io.LineReader;

@Beta   
public class BundleMaker {

    final static String MANIFEST_PATH = JarFile.MANIFEST_NAME;
    private Framework framework;
    private ResourceUtils resources;
    private Class<?> optionalDefaultClassForLoading;

    /** Constructor for use when not expecting to use with a framework */
    public BundleMaker(@Nonnull ResourceUtils resources) {
        this.resources = resources;
    }
    
    public BundleMaker(@Nonnull Framework f, @Nonnull ResourceUtils resources) {
        this.framework = f;
        this.resources = resources;
    }
    
    public BundleMaker(@Nonnull ManagementContext mgmt) {
        this(((ManagementContextInternal) mgmt).getOsgiManager().get().getFramework(), ResourceUtils.create());
    }

    /** if set, this will be used to resolve relative classpath fragments;
     * the {@link ResourceUtils} supplied in the constructor must also be with respect to the given class */
    public void setDefaultClassForLoading(Class<?> optionalDefaultClassForLoading) {
        this.optionalDefaultClassForLoading = optionalDefaultClassForLoading;
    }
    
    /** creates a ZIP in a temp file from the given classpath folder, 
     * by recursively taking everything in the referenced directories,
     * treating the given folder as the root,
     * respecting the MANIFEST.MF if present (ie putting it first so it is a valid JAR) */
    public File createJarFromClasspathDir(String path) {
        File f = Os.newTempFile(path, "zip");
        ZipOutputStream zout = null;
        try {
            if (Urls.getProtocol(path)==null) {
                // default if no URL is classpath
                path = Os.tidyPath(path);
                if (!path.startsWith("/") && optionalDefaultClassForLoading!=null) {
                    path = "/"+optionalDefaultClassForLoading.getPackage().getName().replace('.', '/') + "/" + path;
                }
                path = "classpath:"+path;
            }
            
            if (resources.doesUrlExist(Urls.mergePaths(path, MANIFEST_PATH))) {
                InputStream min = resources.getResourceFromUrl(Urls.mergePaths(path, MANIFEST_PATH));
                zout = new JarOutputStream(new FileOutputStream(f), new Manifest(min));
                addUrlItemRecursively(zout, path, path, Predicates.not(Predicates.equalTo(MANIFEST_PATH)));
            } else {
                zout = new JarOutputStream(new FileOutputStream(f));
                addUrlItemRecursively(zout, path, path, Predicates.alwaysTrue());
            }
            
            return f;
            
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Error creating ZIP from classpath spec "+path, e);
            
        } finally {
            Streams.closeQuietly(zout);
        }
    }

    /** true iff given ZIP/JAR file contains a MANIFEST.MF file defining a bundle symbolic name */
    public boolean hasOsgiManifest(File f) {
        Manifest mf = getManifest(f);
        if (mf==null) return false;
        String sn = mf.getMainAttributes().getValue(Constants.BUNDLE_SYMBOLICNAME);
        return Strings.isNonBlank(sn);
    }

    /** returns the manifest in a JAR file, or null if no manifest contained therein */
    public Manifest getManifest(File f) {
        JarFile jf = null;
        try {
            jf = new JarFile(f);
            return jf.getManifest();
        } catch (IOException e) {
            throw Exceptions.propagateAnnotated("Unable to read "+f+" when looking for manifest", e);
        } finally {
            Streams.closeQuietly(jf);
        }
    }
    
    /** as {@link #copyAddingManifest(File, Manifest)} but taking manifest entries as a map for convenience */
    public File copyAddingManifest(File f, Map<String,String> attrs) {
        return copyAddingManifest(f, manifestOf(attrs));
    }

    protected Manifest manifestOf(Map<String, String> attrs) {
        Manifest mf = new Manifest();
        for (Map.Entry<String,String> attr: attrs.entrySet()) {
            mf.getMainAttributes().putValue(attr.getKey(), attr.getValue());
        }
        return mf;
    }
    
    /** create a copy of the given ZIP as a JAR with the given manifest, returning the new temp file */
    public File copyAddingManifest(File f, Manifest mf) {
        File f2 = Os.newTempFile(f.getName(), "zip");
        ZipOutputStream zout = null;
        ZipFile zf = null;
        try {
            zout = new JarOutputStream(new FileOutputStream(f2), mf);
            writeZipEntriesFromFile(zout, f, Predicates.not(Predicates.equalTo(MANIFEST_PATH)));
        } catch (IOException e) {
            throw Exceptions.propagateAnnotated("Unable to read "+f+" when looking for manifest", e);
        } finally {
            Streams.closeQuietly(zf);
            Streams.closeQuietly(zout);
        }
        return f2;
    }
    
    /** create a copy of the given ZIP as a JAR with the given entries added at the end (removing any duplicates), returning the new temp file */
    public File copyAdding(File f, Map<ZipEntry, ? extends InputStream> entries) {
        return copyAdding(f, entries, Predicates.<String>alwaysTrue(), false);
    }
    
    /** create a copy of the given ZIP as a JAR with the given entries added at the end, returning the new temp file */
    public File copyAddingAtEnd(File f, Map<ZipEntry, ? extends InputStream> entries) {
        return copyAdding(f, entries, Predicates.<String>alwaysTrue(), true);
    }

    /** create a copy of the given ZIP as a JAR with the given entries removed, returning the new temp file */
    public File copyRemoving(File f, final Set<String> itemsToRemove) {
        return copyRemoving(f, new Predicate<String>(){
            @Override
            public boolean apply(String input) {
                return !itemsToRemove.contains(input);
            }
        });
    }
    
    /** create a copy of the given ZIP as a JAR with the given entries removed, returning the new temp file */
    public File copyRemoving(File f, Predicate<? super String> filter) {
        return copyAdding(f, MutableMap.<ZipEntry,InputStream>of(), filter, true);
    }
    
    private File copyAdding(File f, Map<ZipEntry, ? extends InputStream> entries, final Predicate<? super String> filter, boolean addAtStart) {
        final Set<String> entryNames = MutableSet.of();
        for (ZipEntry ze: entries.keySet()) {
            entryNames.add(ze.getName());
        }
        
        File f2 = Os.newTempFile(f.getName(), "zip");
        ZipOutputStream zout = null;
        ZipFile zf = null;
        try {
            zout = new ZipOutputStream(new FileOutputStream(f2));
            
            if (addAtStart) {
                writeZipEntries(zout, entries);
            }
            
            writeZipEntriesFromFile(zout, f, new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return filter.apply(input) && !entryNames.contains(input);
                }
            });
            
            if (!addAtStart) {
                writeZipEntries(zout, entries);
            }

            return f2;
        } catch (IOException e) {
            throw Exceptions.propagateAnnotated("Unable to read "+f+" when looking for manifest", e);
        } finally {
            Streams.closeQuietly(zf);
            Streams.closeQuietly(zout);
        }
    }

    private void writeZipEntries(ZipOutputStream zout, Map<ZipEntry, ? extends InputStream> entries) throws IOException {
        for (Map.Entry<ZipEntry,? extends InputStream> ze: entries.entrySet()) {
            zout.putNextEntry(ze.getKey());
            InputStream zin = ze.getValue();
            Streams.copy(zin, zout);
            Streams.closeQuietly(zin);
            zout.closeEntry();
        }
    }

    private void writeZipEntriesFromFile(ZipOutputStream zout, File existingZip, Predicate<? super String> filter) throws IOException {
        ZipFile zf = new ZipFile(existingZip);
        try {
            Enumeration<? extends ZipEntry> zfe = zf.entries();
            while (zfe.hasMoreElements()) {
                ZipEntry ze = zfe.nextElement();
                ZipEntry newZipEntry = new ZipEntry(ze.getName());
                if (filter.apply(ze.getName())) {
                    zout.putNextEntry(newZipEntry);
                    InputStream zin = zf.getInputStream(ze);
                    Streams.copy(zin, zout);
                    Streams.closeQuietly(zin);
                    zout.closeEntry();
                }
            }
        } finally {
            Streams.closeQuietly(zf);
        }
    }
    
    /** installs the given JAR file as an OSGi bundle; all manifest info should be already set up.
     * bundle-start semantics are TBD.
     * 
     * @deprecated since 0.12.0, use {@link OsgiManager#installUploadedBundle(org.apache.brooklyn.api.typereg.ManagedBundle, InputStream)}*/
    @Deprecated
    public Bundle installBundle(File f, boolean start) {
        try {
            Bundle b = Osgis.install( framework, "file://"+f.getAbsolutePath() );
            if (start) {
                // benefits of start:
                // a) we get wiring issues thrown here, and
                // b) catalog.bom in root will be scanned synchronously here
                // however drawbacks:
                // c) other code doesn't always do it (method above)
                // d) heavier-weight earlier
                // e) tests in IDE break (but mvn fine)
                b.start();
            }
            
            return b;
            
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Error starting bundle from "+f, e);
        }
    }
    
    private boolean addUrlItemRecursively(ZipOutputStream zout, String root, String item, Predicate<? super String> filter) throws IOException {
        InputStream itemFound = null;
        try {
            itemFound = resources.getResourceFromUrl(item);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return false;
        }
        try {
            // can't reliably tell if item a file or a folder (listing files), esp w classpath where folder is treated as a list of files, 
            // so if we can't tell try it as a list of files; not guaranteed, and empty dir and a file of size 0 will appear identical, but better than was
            // (mainly used for tests)
            if (isKnownNotToBeADirectoryListing(item) || !addUrlDirToZipRecursively(zout, root, item, itemFound, filter)) {
                addUrlFileToZip(zout, root, item, filter);
            }
            return true;
        } finally {
            Streams.closeQuietly(itemFound);
        }
    }

    private boolean isKnownNotToBeADirectoryListing(String item) {
        try { 
            URL url = new URL(item);
            if (url.getProtocol().equals("file")) {
                return !new File(url.getFile()).isDirectory();
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // ignore otherwise -- probably unknown protocol
        }
        return false;
    }

    private boolean addUrlDirToZipRecursively(ZipOutputStream zout, String root, String item, InputStream itemFound, Predicate<? super String> filter) throws IOException {
        LineReader lr = new LineReader(new InputStreamReader(itemFound));
        boolean readSubdirFile = false;
        while (true) {
            String line = lr.readLine();
            if (line==null) {
                // at end of file return true if we were able to recurse, else false
                return readSubdirFile;
            }
            boolean isFile = addUrlItemRecursively(zout, root, item+"/"+line, filter);
            if (isFile) {
                readSubdirFile = true;
            } else {
                if (!readSubdirFile) {
                    // not a folder
                    return false;
                } else {
                    // previous entry suggested it was a folder, but this one didn't work! -- was a false positive
                    // but zip will be in inconsistent state, so throw
                    throw new IllegalStateException("Failed to read entry "+line+" in "+item+" but previous entry implied it was a directory");
                }
            }
        }
    }
    
    private void addUrlFileToZip(ZipOutputStream zout, String root, String item, Predicate<? super String> filter) throws IOException {
        int startPos = item.indexOf(root);
        if (startPos<0) {
            throw new IllegalStateException("URL of "+item+" does not appear relative to root "+root);
        }
        String itemE = item.substring(startPos + root.length());
        itemE = Strings.removeFromStart(itemE, "/");
        
        if (Strings.isEmpty(itemE)) {
            // Can happen if we're given an empty folder. addUrlDirToZipRecursively will have returned false, so 
            // will try to add it as a file.
            return; 
        }
        if (!filter.apply(itemE)) {
            return;
        }
        
        InputStream itemFound = null;
        try {
            itemFound = resources.getResourceFromUrl(item);

            ZipEntry e = new ZipEntry(itemE);
            zout.putNextEntry(e);
            Streams.copy(itemFound, zout);
            zout.closeEntry();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            Streams.closeQuietly(itemFound);
        }
    }

    /** Creates a temporary file with the given metadata */ 
    public File createTempBundle(String nameHint, Manifest mf, Map<ZipEntry, InputStream> files) {
        File f2 = Os.newTempFile(nameHint, "zip");
        ZipOutputStream zout = null;
        ZipFile zf = null;
        try {
            zout = mf!=null ? new JarOutputStream(new FileOutputStream(f2), mf) : new ZipOutputStream(new FileOutputStream(f2));
            writeZipEntries(zout, files);
        } catch (IOException e) {
            throw Exceptions.propagateAnnotated("Unable to read/write for "+nameHint, e);
        } finally {
            Streams.closeQuietly(zf);
            Streams.closeQuietly(zout);
        }
        return f2;
    }

    public File createTempBundle(String nameHint, Map<String, String> mf, Map<ZipEntry, InputStream> files) {
        return createTempBundle(nameHint, manifestOf(mf), files);
    }
    
    public File createTempZip(String nameHint, Map<ZipEntry, InputStream> files) {
        return createTempBundle(nameHint, (Manifest)null, files);
    }
    
}
