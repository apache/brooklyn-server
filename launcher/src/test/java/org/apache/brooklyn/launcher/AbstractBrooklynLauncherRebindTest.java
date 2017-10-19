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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.osgi.framework.Constants;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public abstract class AbstractBrooklynLauncherRebindTest {

    protected static final String CATALOG_EMPTY_INITIAL = "classpath://rebind-test-empty-catalog.bom";

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
        return newTmpBundle(files, bundleName, ImmutableMap.of());
    }
    
    protected File newTmpBundle(Map<String, byte[]> files, VersionedName bundleName, Map<String, String> manifestLines) {
        Map<ZipEntry, InputStream> zipEntries = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> entry : files.entrySet()) {
            zipEntries.put(new ZipEntry(entry.getKey()), new ByteArrayInputStream(entry.getValue()));
        }
        
        BundleMaker bundleMaker = new BundleMaker(new ResourceUtils(this));
        File bf = bundleMaker.createTempZip("test", zipEntries);
        tmpFiles.add(bf);
        
        if (bundleName != null || manifestLines.size() > 0) {
            Map<String, String> manifestAllLines = MutableMap.<String, String>builder()
                    .putAll(manifestLines)
                    .putIfAbsent("Manifest-Version", "2.0")
                    .build();
            if (bundleName != null) {
                manifestAllLines.putIfAbsent(Constants.BUNDLE_SYMBOLICNAME, bundleName.getSymbolicName());
                manifestAllLines.putIfAbsent(Constants.BUNDLE_VERSION, bundleName.getOsgiVersion().toString());
            }
            bf = bundleMaker.copyAddingManifest(bf, manifestAllLines);
            tmpFiles.add(bf);
        }
        return bf;
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

    protected void assertManagedBundle(BrooklynLauncher launcher, VersionedName bundleId, Set<VersionedName> expectedCatalogItems) {
        ManagementContextInternal mgmt = (ManagementContextInternal)launcher.getManagementContext();
        ManagedBundle bundle = mgmt.getOsgiManager().get().getManagedBundle(bundleId);
        assertNotNull(bundle, bundleId+" not found");
        
        Set<VersionedName> actualCatalogItems = new LinkedHashSet<>();
        Iterable<RegisteredType> types = launcher.getManagementContext().getTypeRegistry().getAll();
        for (RegisteredType type : types) {
            if (Objects.equal(bundleId.toOsgiString(), type.getContainingBundle())) {
                actualCatalogItems.add(type.getVersionedName());
            }
        }
        assertEquals(actualCatalogItems, expectedCatalogItems, "actual="+actualCatalogItems+"; expected="+expectedCatalogItems);
    }

    private static <T> boolean compareIterablesWithoutOrderMatters(Iterable<T> a, Iterable<T> b) {
        List<T> aList = Lists.newArrayList(a);
        List<T> bList = Lists.newArrayList(b);

        return aList.containsAll(bList) && bList.containsAll(aList);
    }
    
    protected void initPersistedState(Map<String, String> legacyCatalogContents) throws Exception {
        CatalogInitialization catalogInitialization = new CatalogInitialization(CATALOG_EMPTY_INITIAL);
        BrooklynLauncher launcher = newLauncherForTests()
                .catalogInitialization(catalogInitialization);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of());
        launcher.terminate();
        
        for (Map.Entry<String, String> entry : legacyCatalogContents.entrySet()) {
            addMemento(BrooklynObjectType.CATALOG_ITEM, entry.getKey(), entry.getValue());
        }
    }

    protected void addMemento(BrooklynObjectType type, String id, String contents) throws Exception {
        File persistedFile = getPersistanceFile(type, id);
        Files.write(contents.getBytes(StandardCharsets.UTF_8), persistedFile);
    }

    protected File getPersistanceFile(BrooklynObjectType type, String id) {
        String dir;
        switch (type) {
            case ENTITY: dir = "entities"; break;
            case LOCATION: dir = "locations"; break;
            case POLICY: dir = "policies"; break;
            case ENRICHER: dir = "enrichers"; break;
            case FEED: dir = "feeds"; break;
            case CATALOG_ITEM: dir = "catalog"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(persistenceDir, Os.mergePaths(dir, id));
    }
    
    protected String createLegacyPersistenceCatalogItem(VersionedName itemName) {
        return Joiner.on("\n").join(
                "<catalogItem>",
                "  <brooklynVersion>0.12.0-20170901.1331</brooklynVersion>",
                "  <type>org.apache.brooklyn.core:org.apache.brooklyn.core.catalog.internal.CatalogEntityItemDto</type>",
                "  <id>"+itemName.getSymbolicName()+":"+itemName.getVersionString()+"</id>",
                "  <catalogItemId>"+itemName.getSymbolicName()+":"+itemName.getVersionString()+"</catalogItemId>",
                "  <searchPath class=\"ImmutableList\"/>",
                "  <registeredTypeName>"+itemName.getSymbolicName()+"</registeredTypeName>",
                "  <version>"+itemName.getVersionString()+"</version>",
                "  <planYaml>services: [{type: org.apache.brooklyn.entity.stock.BasicApplication}]</planYaml>",
                "  <libraries class=\"ImmutableList\" reference=\"../searchPath\"/>",
                "  <catalogItemType>ENTITY</catalogItemType>",
                "  <catalogItemJavaType>org.apache.brooklyn.api:org.apache.brooklyn.api.entity.Entity</catalogItemJavaType>",
                "  <specType>org.apache.brooklyn.api:org.apache.brooklyn.api.entity.EntitySpec</specType>",
                "  <deprecated>false</deprecated>",
                "  <disabled>false</disabled>",
                "</catalogItem>");
    }
    
    protected String createCatalogYaml(Iterable<URI> libraries, Iterable<VersionedName> entities) {
        if (Iterables.isEmpty(libraries) && Iterables.isEmpty(entities)) {
            return "brooklyn.catalog: {}\n";
        }
        
        StringBuilder result = new StringBuilder();
        result.append("brooklyn.catalog:\n");
        if (!Iterables.isEmpty(libraries)) {
            result.append("  brooklyn.libraries:\n");
        }
        for (URI library : libraries) {
            result.append("    - " + library+"\n");
        }
        if (!Iterables.isEmpty(entities)) {
            result.append("  items:\n");
        }
        for (VersionedName entity : entities) {
            result.append("    - id: " + entity.getSymbolicName()+"\n");
            result.append("      version: " + entity.getVersionString()+"\n");
            result.append("      itemType: entity"+"\n");
            result.append("      item:"+"\n");
            result.append("        type: " + BasicEntity.class.getName()+"\n");
        }
        return result.toString();
    }
}
