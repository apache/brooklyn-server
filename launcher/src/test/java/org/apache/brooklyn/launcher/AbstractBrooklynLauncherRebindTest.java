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
package org.apache.brooklyn.launcher;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.collections.IteratorUtils;
import org.osgi.framework.Constants;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public abstract class AbstractBrooklynLauncherRebindTest {

    protected List<BrooklynLauncher> launchers = Lists.newCopyOnWriteArrayList();
    protected List<File> tmpFiles = new ArrayList<>();

    protected String persistenceDir;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        launchers.clear();
        tmpFiles.clear();
        persistenceDir = newTempPersistenceContainerName();
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        for (File file : tmpFiles) {
            if (file.exists()) file.delete();
        }
        for (BrooklynLauncher launcher : launchers) {
            launcher.terminate();
        }
        launchers.clear();
        if (persistenceDir != null) Os.deleteRecursively(persistenceDir);
    }

    protected boolean useOsgi() {
        return false;
    }
    
    protected boolean reuseOsgi() {
        return true;
    }
    
    protected BrooklynLauncher newLauncherForTests() {
        return newLauncherForTests(PersistMode.AUTO, HighAvailabilityMode.DISABLED);
    }
    
    protected BrooklynLauncher newLauncherForTests(PersistMode persistMode, HighAvailabilityMode haMode) {
        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .brooklynProperties(LocalManagementContextForTests.builder(true).setOsgiEnablementAndReuse(useOsgi(), reuseOsgi()).buildProperties())
                .persistMode(persistMode)
                .highAvailabilityMode(haMode)
                .persistPeriod(Duration.millis(10))
                .haHeartbeatPeriod(Duration.millis(10))
                .persistenceDir(persistenceDir)
                .webconsole(false);
        launchers.add(launcher);
        return launcher;
    }

    protected String newTempPersistenceContainerName() {
        File persistenceDirF = Files.createTempDir();
        Os.deleteOnExitRecursively(persistenceDirF);
        return persistenceDirF.getAbsolutePath();
    }
    
    protected File newTmpFile(String contents) throws Exception {
        File file = java.nio.file.Files.createTempFile("brooklynLauncherRebindTest-"+Identifiers.makeRandomId(4), "txt").toFile();
        tmpFiles.add(file);
        Files.write(contents, file, Charsets.UTF_8);
        return file;
    }
    
    protected File newTmpBundle(Map<String, byte[]> files, VersionedName bundleName) {
        Map<ZipEntry, InputStream> zipEntries = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> entry : files.entrySet()) {
            zipEntries.put(new ZipEntry(entry.getKey()), new ByteArrayInputStream(entry.getValue()));
        }
        
        BundleMaker bundleMaker = new BundleMaker(new ResourceUtils(this));
        File bf = bundleMaker.createTempZip("test", zipEntries);
        tmpFiles.add(bf);
        
        if (bundleName!=null) {
            bf = bundleMaker.copyAddingManifest(bf, MutableMap.of(
                "Manifest-Version", "2.0",
                Constants.BUNDLE_SYMBOLICNAME, bundleName.getSymbolicName(),
                Constants.BUNDLE_VERSION, bundleName.getOsgiVersion().toString()));
            tmpFiles.add(bf);
        }
        return bf;
        
//        ReferenceWithError<OsgiBundleInstallationResult> b = ((ManagementContextInternal)mgmt).getOsgiManager().get().install(
//                new FileInputStream(bf) );
//
//            b.checkNoError();

    }
    
    protected void assertCatalogConsistsOfIds(BrooklynLauncher launcher, Iterable<VersionedName> ids) {
        BrooklynTypeRegistry typeRegistry = launcher.getServerDetails().getManagementContext().getTypeRegistry();
        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        assertTypeRegistryConsistsOfIds(typeRegistry.getAll(), ids);
        assertCatalogConsistsOfIds(catalog.getCatalogItems(), ids);
    }

    protected void assertCatalogConsistsOfIds(Iterable<CatalogItem<Object, Object>> catalogItems, Iterable<VersionedName> ids) {
        Iterable<VersionedName> idsFromItems = Iterables.transform(catalogItems, new Function<CatalogItem<?,?>, VersionedName>() {
            @Nullable
            @Override
            public VersionedName apply(CatalogItem<?, ?> input) {
                return VersionedName.fromString(input.getCatalogItemId());
            }
        });
        Assert.assertTrue(compareIterablesWithoutOrderMatters(ids, idsFromItems), String.format("Expected %s, found %s", ids, idsFromItems));
    }

    protected void assertTypeRegistryConsistsOfIds(Iterable<RegisteredType> types, Iterable<VersionedName> ids) {
        Iterable<VersionedName> idsFromItems = Iterables.transform(types, new Function<RegisteredType, VersionedName>() {
            @Nullable
            @Override
            public VersionedName apply(RegisteredType input) {
                return input.getVersionedName();
            }
        });
        Assert.assertTrue(compareIterablesWithoutOrderMatters(ids, idsFromItems), String.format("Expected %s, found %s", ids, idsFromItems));
    }

    private static <T> boolean compareIterablesWithoutOrderMatters(Iterable<T> a, Iterable<T> b) {
        List<T> aList = IteratorUtils.toList(a.iterator());
        List<T> bList = IteratorUtils.toList(b.iterator());

        return aList.containsAll(bList) && bList.containsAll(aList);
    }
}
