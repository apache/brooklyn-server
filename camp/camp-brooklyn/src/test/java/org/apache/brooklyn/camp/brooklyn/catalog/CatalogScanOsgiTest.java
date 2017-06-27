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

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.test.lite.CampYamlLiteTest;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
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
        
        ((ManagementContextInternal)mgmt).getOsgiManager().get().install(new FileInputStream(f)).checkNoError();
    }
    
    @Test
    public void testScanLegacyListedLibraries() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH);
        addCatalogItems(bomForLegacySiblingLibraries());

        RegisteredType hereItem = mgmt().getTypeRegistry().get("here-item");
        assertEquals(hereItem.getVersion(), "2.0-test_java");
        assertEquals(hereItem.getLibraries().size(), 3);
        assertEquals(hereItem.getContainingBundle(), "test-items:2.0-test_java");
        
        RegisteredType item = mgmt().getTypeRegistry().get(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        // versions and libraries _are_ inherited in this legacy mode
        assertEquals(item.getVersion(), "2.0-test_java");
        assertEquals(item.getLibraries().size(), 3);
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
    
}
