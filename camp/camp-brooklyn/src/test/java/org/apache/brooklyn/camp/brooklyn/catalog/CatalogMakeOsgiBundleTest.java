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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.internal.CatalogBomScanner;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class CatalogMakeOsgiBundleTest extends AbstractYamlTest {

    private BundleMaker bm;
    List<Bundle> bundlesToRemove = MutableList.of();

    @Override
    protected LocalManagementContext newTestManagementContext() {
        return LocalManagementContextForTests.builder(true)
                .disableOsgi(false)
                .build();
    }
    
    // keep OSGi framework around for all tests
    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        manuallyEnableBomScanner(mgmt());
        bm = new BundleMaker( ((LocalManagementContext)mgmt()).getOsgiManager().get().getFramework(), ResourceUtils.create(this) );
    }
    
    // just clean it up between tests, to speed things up
    @AfterMethod(alwaysRun = true)
    public void cleanUpButKeepMgmt() throws Exception {
        for (Application app: MutableList.copyOf(mgmt().getApplications())) {
            Entities.destroy(app);
        }
        for (Bundle b: bundlesToRemove) {
            b.uninstall();
        }
        bundlesToRemove.clear();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    public static void manuallyEnableBomScanner(ManagementContext mgmt) {
        BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_LOAD_BUNDLE_CATALOG_BOM);
        
        CatalogBomScanner scanner = new CatalogBomScanner();
        BundleContext context = ((LocalManagementContext)mgmt).getOsgiManager().get().getFramework().getBundleContext();
        
        context.registerService(ManagementContext.class.getName(), mgmt, null);
        ServiceReference<ManagementContext> ref = context.getServiceReference(ManagementContext.class);
        try {
            scanner.bind(ref);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @AfterMethod(alwaysRun=true)
    public void clearFeatureEnablement() throws Exception {
        BrooklynFeatureEnablement.clearCache();
    }

    @Test
    public void testCatalogBomFromBundleWithManifest() throws Exception {
        bm.setDefaultClassForLoading(getClass());
        File jf = bm.createJarFromClasspathDir("osgi/catalog-bundle-1");
        
        Assert.assertTrue(bm.hasOsgiManifest(jf));
        
        installBundle(jf);
        assertHasBasic1();
        assertBasic1DeploysAndHasSensor();
    }

    private Entity assertBasic1DeploysAndHasSensor() throws Exception {
        String yaml = "name: simple-app-yaml\n" +
                "services: \n" +
                "- type: " + "basic1";
        Entity app = createAndStartApplication(yaml);
        Entity basic1 = Iterables.getOnlyElement( app.getChildren() );
        EntityAsserts.assertAttribute(basic1, Sensors.newStringSensor("a.sensor"), Predicates.equalTo("A"));
        
        return basic1;
    }

    private void assertHasBasic1() {
        RegisteredType basic1T = mgmt().getTypeRegistry().get("basic1");
        Asserts.assertNotNull(basic1T, "basic1 not in catalog");
    }

    @Test
    public void testCatalogBomFromBundleWithManualManifest() throws Exception {
        bm.setDefaultClassForLoading(getClass());
        File jf = bm.createJarFromClasspathDir("osgi/catalog-bundle-1");
        jf = bm.copyRemoving(jf, MutableSet.of(JarFile.MANIFEST_NAME));
        String customName = "catalog-bundle-1-manual-"+Identifiers.makeRandomId(4);
        
        jf = bm.copyAddingManifest(jf, MutableMap.of(
                "Manifest-Version", "1.0", 
                "Bundle-SymbolicName", customName));
        
        Assert.assertTrue(bm.hasOsgiManifest(jf));
        
        installBundle(jf);
        assertHasBasic1();
        Entity basic1 = assertBasic1DeploysAndHasSensor();
        
        RegisteredType item = mgmt().getTypeRegistry().get( basic1.getCatalogItemId() );
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        Asserts.assertSize(libs, 1);
        Assert.assertEquals(Iterables.getOnlyElement(libs).getSymbolicName(), customName);
    }

    private void installBundle(File jf) {
        Bundle bundle = bm.installBundle(jf, true);
        bundlesToRemove.add(bundle);
    }
    
    @Test
    public void testCatalogBomLoadsFileInBundle() throws Exception {
        bm.setDefaultClassForLoading(getClass());
        File jf = bm.createJarFromClasspathDir("osgi/catalog-bundle-1");
        
        // add a file in the bundle
        String customText = "Sample data "+Identifiers.makeRandomId(4);
        jf = bm.copyAdding(jf, MutableMap.of(
                new ZipEntry("sample.txt"), (InputStream) new ByteArrayInputStream(customText.getBytes())));
        
        installBundle(jf);

        String yaml = Strings.lines("name: simple-app-yaml",
                "services:",
                "- type: " + "basic1",
                "  brooklyn.initializers:",
                "  - type: "+GetFileContentsEffector.class.getName());
        Entity app = createAndStartApplication(yaml);
        Entity basic1 = Iterables.getOnlyElement( app.getChildren() );
        
        // check the file put in the bundle gets loaded without needing to do anything special
        String contents = basic1.invoke(GetFileContentsEffector.GET_FILE_CONTENTS, MutableMap.of(GetFileContentsEffector.FILENAME.getName(), "classpath://sample.txt")).get();
        Asserts.assertEquals(contents, customText);
    }
    
}
