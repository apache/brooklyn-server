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
package org.apache.brooklyn.camp.brooklyn.catalog;

import java.util.function.Predicate;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult.ResultCode;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.InputStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.test.lite.CampYamlLiteTest;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class CatalogScanOsgiTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogScanOsgiTest.class);

    @Override protected boolean disableOsgi() { return false; }
    
    @Test(groups="Broken")  // AH think not going to support this; see notes in BasicBrooklynCatalog.scanAnnotationsInBundle
    public void testScanContainingBundle() throws Exception {
        installJavaScanningMoreEntitiesV2(mgmt(), this);
        
        RegisteredType item = mgmt().getTypeRegistry().get(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        Assert.assertNotNull(item, "Scanned item should have been loaded but wasn't");
        assertEquals(item.getVersion(), "0.2.0");
        assertEquals(item.getDisplayName(), "More Entity v2");
        assertEquals(item.getContainingBundle(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL+":"+"0.2.0");
        assertEquals(Iterables.getOnlyElement(item.getLibraries()).getVersionedName().toString(), 
            OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL+":"+"0.2.0");
    }

    static void installJavaScanningMoreEntitiesV2(ManagementContext mgmt, Object context) throws FileNotFoundException {
        // scanning bundle functionality added in 0.12.0, relatively new compared to non-osgi scanning
        
        TestResourceUnavailableException.throwIfResourceUnavailable(context.getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        TestResourceUnavailableException.throwIfResourceUnavailable(context.getClass(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH);
        
        CampYamlLiteTest.installWithoutCatalogBom(mgmt, OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        
        BundleMaker bm = new BundleMaker(mgmt);
        File f = Os.newTempFile(context.getClass(), "jar");
        Streams.copy(ResourceUtils.create(context).getResourceFromUrl(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH), new FileOutputStream(f));
        f = bm.copyRemoving(f, MutableSet.of("catalog.bom"));
        f = bm.copyAdding(f, MutableMap.of(new ZipEntry("catalog.bom"),
            new ByteArrayInputStream( Strings.lines(
                "brooklyn.catalog:",
                "  scanJavaAnnotations: true").getBytes() ) ));
        
        ((ManagementContextInternal)mgmt).getOsgiManager().get().install(InputStreamSource.of("test:"+f, f)).checkNoError();
    }
    
    @Test
    public void testScanLegacyListedLibraries() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH);
        addCatalogItems(bomForLegacySiblingLibraries());

        RegisteredType hereItem = mgmt().getTypeRegistry().get("here-item");
        assertEquals(hereItem.getVersion(), "2.0-test_java");
        Asserts.assertSize(hereItem.getLibraries(), 2);
        assertEquals(hereItem.getContainingBundle(), "test-items:2.0-test_java");
        
        RegisteredType item = mgmt().getTypeRegistry().get(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        // versions and libraries _are_ inherited in this legacy mode
        assertEquals(item.getVersion(), "2.0-test_java");
        Asserts.assertSize(hereItem.getLibraries(), 2);
        // and the containing bundle is recorded as the 
        assertEquals(item.getContainingBundle(), "test-items"+":"+"2.0-test_java");
    }

    static String bomForLegacySiblingLibraries() {
        return Strings.lines("brooklyn.catalog:",
            "    bundle: test-items",
            "    version: 2.0-test_java",
            "    items:",
            "    - scanJavaAnnotations: true",
            "      item:",
            "        id: here-item",
            "        type: "+OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY,
            "      libraries:",
            "      - classpath://" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH,
            "      - classpath://" + OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH);
    }
    
    @Test
    public void testAddAnonymousBomTwiceDeletesOldEmptyOne() {
        Map<String, ManagedBundle> b1 = ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundles();
        addCatalogItems(bomAnonymous());
        Map<String, ManagedBundle> b2_new = MutableMap.copyOf(
            ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundles() );
        for (String old: b1.keySet()) b2_new.remove(old);

        RegisteredType sample = mgmt().getTypeRegistry().get("sample");
        Asserts.assertNotNull(sample);
        Asserts.assertSize(b2_new.values(), 1);
        
        addCatalogItems(bomAnonymous());
        Map<String, ManagedBundle> b3 = MutableMap.copyOf(
            ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundles() );
        Map<String, ManagedBundle> b3_new = MutableMap.copyOf(b3);
        for (String old: b1.keySet()) b3_new.remove(old);
        for (String old: b2_new.keySet()) b3_new.remove(old);
        Asserts.assertSize(b3_new.values(), 1);

        Asserts.assertFalse(b3_new.keySet().contains( Iterables.getOnlyElement(b2_new.keySet()) ));
    }

    static String bomAnonymous() {
        return Strings.lines("brooklyn.catalog:",
                "    items:",
                "    - item:",
                "        id: sample",
                "        type: "+BasicEntity.class.getName());
    }

    @Test
    public void testAddOverwritingSnapshotBoms() throws IOException {
        installBom(bom("aaa", "2-SNAPSHOT", 1), ResultCode.INSTALLED_NEW_BUNDLE);
        checkInstalled("aaa", 1);

        installBom(bom("aaa", "2-SNAPSHOT", 2), ResultCode.UPDATED_EXISTING_BUNDLE);
        checkInstalled("aaa", 2);

        // fixed by ea4518ed7ee018d7af2996ec8da44ac3780c4469 - previously the bundle would be ignored
        installBom(bom("aaa", "2-SNAPSHOT", 1), ResultCode.UPDATED_EXISTING_BUNDLE);
        checkInstalled("aaa", 1);

        // but this fails with ea4518ed7ee018d7af2996ec8da44ac3780c4469 - now the bundle is reinstalled
        installBom(bom("aaa", "2-SNAPSHOT", 1), ResultCode.IGNORING_BUNDLE_AREADY_INSTALLED);
        checkInstalled("aaa", 1);
    }

    @Test
    public void testAddOverwritingSnapshotAndNonSnapshotBoms() throws IOException {
        installBom(bomSnapshot("bbb"));
        checkInstalled("bbb", null);

        // overwrites snapshot
        installBom(bom("bbb", "1.0", 1));
        checkInstalled("bbb", 1);

        // same one can be installed
        installBom(bom("bbb", "1.0", 1));
        checkInstalled("bbb", 1);

        // different one at non-snapshot version cannot be reinstalled
        Asserts.assertFailsWith(() -> installBom(bom("bbb", "1.0", 2)), e -> {
            Asserts.expectedFailureContainsIgnoreCase(e, "already installed", "bbb");
            return true;
        });

        // new snapshot can be installed but is not preferred
        installBom(bom("bbb", "3.0-SNAPSHOT", 2));
        checkInstalled("bbb", 1);

    }

    public void installBom(String bom) {
        installBom(bom, null);
    }

    public void installBom(String bom, ResultCode expectedResultCode) {
        try {
            File f = Os.newTempFile(this.getClass(), "zip");
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
            out.putNextEntry(new ZipEntry("catalog.bom"));
            Streams.copy(new ByteArrayInputStream(bom.getBytes(StandardCharsets.UTF_8)), out);
            out.closeEntry();
            Streams.closeQuietly(out);

            OsgiBundleInstallationResult result = ((ManagementContextInternal) mgmt()).getOsgiManager().get().install(InputStreamSource.of("test:" + f, f)).getWithError();
            LOG.info("Installed "+result.getVersionedName()+": "+result.getCode()+" - "+result.getMessage());
            if (expectedResultCode!=null) Asserts.assertEquals(result.getCode(), expectedResultCode);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    public void checkInstalled(String name, Integer fooConfig) throws IOException {
        RegisteredType t = mgmt().getTypeRegistry().get(name);
        Asserts.assertNotNull(t, "Could not find type '"+name+"'");
        Asserts.assertEquals(t.getDisplayName(), name);
        EntitySpec spec = (EntitySpec) mgmt().getTypeRegistry().create(t, null, null);
        Object realFooConfig = spec.getConfig().get(ConfigKeys.newConfigKey(Object.class, "foo"));
        Asserts.assertEquals(realFooConfig, fooConfig);
    }

    static String bomSnapshot(String name) {
        return bom(name, "2.0-SNAPSHOT");
    }
    static String bom(String name, String version) {
        return Strings.lines("brooklyn.catalog:",
            "    version: "+version,
            "    bundle: "+name,
            "    items:",
            "    - item:",
            "        id: "+name,
            "        name: "+name,
            "        displayName: "+name,
            "        type: "+BasicEntity.class.getName());
    }

    static String bom(String name, String version, Integer fooConfig) {
        return Strings.lines(bom(name, version),
                "        brooklyn.config:",
                "          foo: "+fooConfig);
    }

    
}
