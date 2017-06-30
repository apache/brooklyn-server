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
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.zip.ZipEntry;

import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

/** Variant of parent tests using OSGi, bundles, and type registry, instead of lightweight non-osgi catalog */
@Test
public class CatalogYamlEntityOsgiTypeRegistryTest extends CatalogYamlEntityTest {

    // use OSGi here
    @Override protected boolean disableOsgi() { return false; }
    
    // use type registry appraoch
    @Override
    protected void addCatalogItems(String catalogYaml) {
        try {
            BundleMaker bundleMaker = new BundleMaker(mgmt());
            File bf = bundleMaker.createTempZip("test", MutableMap.of(
                new ZipEntry(BasicBrooklynCatalog.CATALOG_BOM), new ByteArrayInputStream(catalogYaml.getBytes())));
            ReferenceWithError<OsgiBundleInstallationResult> b = ((ManagementContextInternal)mgmt()).getOsgiManager().get().installDeferredStart(
                new BasicManagedBundle(bundleName(), bundleVersion(), null), 
                new FileInputStream(bf),
                false);
            // bundle not started (no need), and BOM not installed nor validated above; 
            // do BOM install and validation below manually to test the type registry approach
            mgmt().getCatalog().addTypesFromBundleBom(catalogYaml, b.get().getMetadata(), isForceUpdate());
            Map<RegisteredType, Collection<Throwable>> validation = mgmt().getCatalog().validateTypes( mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(b.get().getVersionedName())) );
            if (!validation.isEmpty()) {
                throw Exceptions.propagate("Brooklyn failed to load types: "+validation.keySet(), 
                    Iterables.concat(validation.values()));
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    protected void deleteCatalogEntity(String catalogItem) {
        mgmt().getCatalog().deleteCatalogItem(catalogItem, TEST_VERSION);
    }

    protected String bundleName() { return "sample-bundle"; }
    protected String bundleVersion() { return BasicBrooklynCatalog.NO_VERSION; }
    
    @Test   // basic test that this approach to adding types works
    public void testAddTypes() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), BasicEntity.class.getName());

        Iterable<RegisteredType> itemsInstalled = mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(new VersionedName(bundleName(), bundleVersion())));
        Asserts.assertSize(itemsInstalled, 1);
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        Asserts.assertEquals(item, Iterables.getOnlyElement(itemsInstalled), "Wrong item; installed: "+itemsInstalled);
    }

    // many other tests from super now run, with the type registry approach instead of catalog item / catalog approach
    
}
