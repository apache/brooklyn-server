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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
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
import com.google.common.collect.ImmutableSet;
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
        return newLauncherForTests(PersistMode.AUTO, HighAvailabilityMode.DISABLED)
            .globalBrooklynPropertiesFile(null);
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
    
    protected File newTmpCopy(File orig) throws Exception {
        File file = java.nio.file.Files.createTempFile("brooklynLauncherRebindTest-"+Identifiers.makeRandomId(4), "txt").toFile();
        tmpFiles.add(file);
        Streams.copyClose(new FileInputStream(orig), new FileOutputStream(file));
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

    BundleFile.Builder bundleBuilder() {
        return new BundleFile.Builder(this);
    }
    
    protected static class BundleFile {
        public static class Builder {
            private final AbstractBrooklynLauncherRebindTest test;
            private VersionedName name;
            private String catalogBom;
            private Map<String, String> manifestLines;

            public Builder(AbstractBrooklynLauncherRebindTest test) {
                this.test = test;
            }
            public Builder name(VersionedName val) {
                this.name = val;
                return this;
            }
            public Builder name(String symbolicName, String version) {
                this.name = new VersionedName(symbolicName, version);
                return this;
            }
            public Builder catalogBom(String val) {
                this.catalogBom = val;
                return this;
            }
            protected Builder catalogBom(Iterable<URI> libraries, Iterable<VersionedName> entities) {
                return catalogBom(libraries, ImmutableList.of(), entities, null);
            }
            protected Builder catalogBom(Iterable<URI> libraryUris, Iterable<VersionedName> libraryNames, Iterable<VersionedName> entities) {
                return catalogBom(libraryUris, libraryNames, entities, null);
            }
            protected Builder catalogBom(Iterable<URI> libraryUris, Iterable<VersionedName> libraryNames, Iterable<VersionedName> entities, String randomNoise) {
                return catalogBom(test.createCatalogYaml(libraryUris, libraryNames, entities, randomNoise));
            }
            public Builder manifestLines(Map<String, String> val) {
                this.manifestLines = val;
                return this;
            }
            public BundleFile build() {
                if (name == null) {
                    name = new VersionedName("com.example.brooklyntests."+Identifiers.makeRandomId(4), "1.0.0");
                }
                Map<String, byte[]> filesInBundle;
                if (catalogBom != null) {
                    filesInBundle = ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, catalogBom.getBytes(StandardCharsets.UTF_8));
                } else {
                    filesInBundle = ImmutableMap.of();
                }
                File file = test.newTmpBundle(filesInBundle, name, manifestLines);
                
                return new BundleFile(name, file);
            }
        }
        
        public final VersionedName name;
        public final File file;
        
        BundleFile(VersionedName name, File file) {
            this.name = name;
            this.file = file;
        }
        
        public VersionedName getVersionedName() {
            return name;
        }
        
        public File getFile() {
            return file;
        }
    }

    protected void assertHealthyMaster(BrooklynLauncher launcher) {
        ManagementContextInternal mgmt = (ManagementContextInternal) launcher.getServerDetails().getManagementContext();
        assertTrue(mgmt.isStartupComplete());
        assertTrue(mgmt.isRunning());
        assertTrue(mgmt.errors().isEmpty(), "errs="+mgmt.errors());
        assertEquals(mgmt.getHighAvailabilityManager().getNodeState(), ManagementNodeState.MASTER);
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

    protected ReferenceWithError<OsgiBundleInstallationResult> installBundle(BrooklynLauncher launcher, File file, boolean force) throws Exception {
        return installBundle(launcher, java.nio.file.Files.readAllBytes(file.toPath()), force);
    }

    protected ReferenceWithError<OsgiBundleInstallationResult> installBundle(BrooklynLauncher launcher, byte[] zipInput, boolean force) {
        ManagementContextInternal mgmt = (ManagementContextInternal)launcher.getManagementContext();
        return mgmt.getOsgiManager().get().install(null, new ByteArrayInputStream(zipInput), true, true, force);
    }
    
    protected ManagedBundle findManagedBundle(BrooklynLauncher launcher, VersionedName bundleId) {
        ManagementContextInternal mgmt = (ManagementContextInternal)launcher.getManagementContext();
        ManagedBundle bundle = mgmt.getOsgiManager().get().getManagedBundle(bundleId);
        assertNotNull(bundle, bundleId+" not found");
        return bundle;
    }
    
    protected void assertManagedBundle(BrooklynLauncher launcher, VersionedName bundleId, Set<VersionedName> expectedCatalogItems) {
        assertNotNull(findManagedBundle(launcher, bundleId), "Bundle "+bundleId+" not found");
        
        Set<VersionedName> actualCatalogItems = new LinkedHashSet<>();
        Iterable<RegisteredType> types = launcher.getManagementContext().getTypeRegistry().getAll();
        for (RegisteredType type : types) {
            if (Objects.equal(bundleId.toOsgiString(), type.getContainingBundle())) {
                actualCatalogItems.add(type.getVersionedName());
            }
        }
        assertEquals(actualCatalogItems, expectedCatalogItems, "actual="+actualCatalogItems+"; expected="+expectedCatalogItems);
    }

    protected void assertNotManagedBundle(BrooklynLauncher launcher, VersionedName bundleId) {
        ManagementContextInternal mgmt = (ManagementContextInternal)launcher.getManagementContext();
        ManagedBundle bundle = mgmt.getOsgiManager().get().getManagedBundle(bundleId);
        assertNull(bundle, bundleId+" should not exist");
    }
    
    protected Set<String> getPersistenceListing(BrooklynObjectType type) throws Exception {
        File persistedSubdir = getPersistanceSubdirectory(type);
        return ImmutableSet.copyOf(persistedSubdir.list((dir,name) -> !name.endsWith(".jar")));
    }

    private File getPersistanceSubdirectory(BrooklynObjectType type) {
        String dir;
        switch (type) {
            case ENTITY: dir = "entities"; break;
            case LOCATION: dir = "locations"; break;
            case POLICY: dir = "policies"; break;
            case ENRICHER: dir = "enrichers"; break;
            case FEED: dir = "feeds"; break;
            case CATALOG_ITEM: dir = "catalog"; break;
            case MANAGED_BUNDLE: dir = "bundles"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(persistenceDir, dir);
    }

    private static <T> boolean compareIterablesWithoutOrderMatters(Iterable<T> a, Iterable<T> b) {
        List<T> aList = Lists.newArrayList(a);
        List<T> bList = Lists.newArrayList(b);

        return aList.containsAll(bList) && bList.containsAll(aList);
    }
    
    protected String createPersistenceManagedBundle(String randomId, VersionedName bundleName) {
        return Joiner.on("\n").join(
                "<managedBundle>",
                "<brooklynVersion>1.0.0-SNAPSHOT</brooklynVersion>",
                "<type>org.apache.brooklyn.core:org.apache.brooklyn.core.typereg.BasicManagedBundle</type>",
                "<id>"+randomId+"</id>",
                "<searchPath class=\"ImmutableList\"/>",
                "<symbolicName>"+bundleName.getSymbolicName()+"</symbolicName>",
                "<version>"+bundleName.getVersionString()+"</version>",
                "<url>http://example.org/brooklyn/"+bundleName.getSymbolicName()+"/"+bundleName.getVersionString()+"</url>",
                "</managedBundle>");
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
        return createCatalogYaml(libraries, ImmutableList.of(), entities, null);
    }

    protected String createCatalogYaml(Iterable<URI> libraryUris, Iterable<VersionedName> libraryNames, Iterable<VersionedName> entities) {
        return createCatalogYaml(libraryUris, libraryNames, entities, null);
    }
    
    protected String createCatalogYaml(Iterable<URI> libraryUris, Iterable<VersionedName> libraryNames, Iterable<VersionedName> entities, String randomNoise) {
        StringBuilder result = new StringBuilder();
        result.append("brooklyn.catalog:\n");
        if (randomNoise != null) {
            result.append("  description: "+randomNoise+"\n");
        }
        if (!(Iterables.isEmpty(libraryUris) && Iterables.isEmpty(libraryNames))) {
            result.append("  brooklyn.libraries:\n");
        }
        for (URI library : libraryUris) {
            result.append("    - " + library+"\n");
        }
        for (VersionedName library : libraryNames) {
            result.append("    - name: "+library.getSymbolicName()+"\n");
            result.append("      version: \""+library.getVersionString()+"\"\n");
        }
        if (Iterables.isEmpty(entities)) {
            result.append("  items: []\n");
        } else {
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
    

    
    
    public PersistedStateInitializer newPersistedStateInitializer() {
        return new PersistedStateInitializer();
    }
    
    public class PersistedStateInitializer {
        private final Map<String, String> legacyCatalogContents = new LinkedHashMap<>();
        private final Map<VersionedName, File> bundles = new LinkedHashMap<>();

        public PersistedStateInitializer legacyCatalogItems(Map<String, String> vals) throws Exception {
            legacyCatalogContents.putAll(vals);
            return this;
        }
        
        public PersistedStateInitializer bundle(VersionedName bundleName, File file) throws Exception {
            bundles.put(bundleName, file);
            return this;
        }
        
        public PersistedStateInitializer bundle(BundleFile bundleFile) throws Exception {
            return bundle(bundleFile.getVersionedName(), bundleFile.getFile());
        }
        
        public PersistedStateInitializer bundles(Map<VersionedName, File> vals) throws Exception {
            bundles.putAll(vals);
            return this;
        }
        
        public void initState() throws Exception {
            initEmptyState();
            
            for (Map.Entry<String, String> entry : legacyCatalogContents.entrySet()) {
                addMemento(BrooklynObjectType.CATALOG_ITEM, entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
            for (Map.Entry<VersionedName, File> entry : bundles.entrySet()) {
                VersionedName bundleName = entry.getKey();
                String randomId = Identifiers.makeRandomId(8);
                String managedBundleXml = createPersistenceManagedBundle(randomId, bundleName);
                addMemento(BrooklynObjectType.MANAGED_BUNDLE, randomId, managedBundleXml.getBytes(StandardCharsets.UTF_8));
                addMemento(BrooklynObjectType.MANAGED_BUNDLE, randomId+".jar", Streams.readFullyAndClose(new FileInputStream(entry.getValue())));
            }
        }

        private void addMemento(BrooklynObjectType type, String id, byte[] contents) throws Exception {
            File persistedFile = getPersistanceFile(type, id);
            Files.createParentDirs(persistedFile);
            Files.write(contents, persistedFile);
        }

        private File getPersistanceFile(BrooklynObjectType type, String id) {
            String dir;
            switch (type) {
                case ENTITY: dir = "entities"; break;
                case LOCATION: dir = "locations"; break;
                case POLICY: dir = "policies"; break;
                case ENRICHER: dir = "enrichers"; break;
                case FEED: dir = "feeds"; break;
                case CATALOG_ITEM: dir = "catalog"; break;
                case MANAGED_BUNDLE: dir = "bundles"; break;
                default: throw new UnsupportedOperationException("type="+type);
            }
            return new File(persistenceDir, Os.mergePaths(dir, id));
        }
        
        private void initEmptyState() {
            CatalogInitialization catalogInitialization = new CatalogInitialization(CATALOG_EMPTY_INITIAL);
            BrooklynLauncher launcher = newLauncherForTests()
                    .catalogInitialization(catalogInitialization);
            launcher.start();
            assertCatalogConsistsOfIds(launcher, ImmutableList.of());
            launcher.terminate();
        }
    }
    
    public static Application createAndStartApplication(ManagementContext mgmt, String input) throws Exception {
        EntitySpec<?> spec = 
            mgmt.getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, input, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = mgmt.getEntityManager().createEntity(spec);
        app.invoke(Startable.START, MutableMap.of()).get();
        return (Application) app;
    }

}
