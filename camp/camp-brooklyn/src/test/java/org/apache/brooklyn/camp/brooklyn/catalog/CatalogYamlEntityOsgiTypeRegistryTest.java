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

import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

/** Variant of parent tests using OSGi, bundles, and type registry, instead of lightweight non-osgi catalog */
@Test
public class CatalogYamlEntityOsgiTypeRegistryTest extends CatalogYamlEntityTest {

    // use OSGi here
    @Override protected boolean disableOsgi() { return false; }
    
    enum CatalogItemsInstallationMode { 
        ADD_YAML_ITEMS_UNBUNDLED, 
        BUNDLE_BUT_NOT_STARTED, 
        USUAL_OSGI_WAY_AS_BUNDLE_WITH_DEFAULT_NAME, 
        USUAL_OSGI_WAY_AS_ZIP_NO_MANIFEST_NAME_MAYBE_IN_BOM 
    }
    CatalogItemsInstallationMode itemsInstallMode = null;
    
    // use type registry approach
    @Override
    protected void addCatalogItems(String catalogYaml) {
        switch (itemsInstallMode!=null ? itemsInstallMode : 
            // this is the default because some "bundles" aren't resolvable or library BOMs loadable in test context
            CatalogItemsInstallationMode.BUNDLE_BUT_NOT_STARTED) {
        case ADD_YAML_ITEMS_UNBUNDLED: super.addCatalogItems(catalogYaml); break;
        case BUNDLE_BUT_NOT_STARTED: 
            addCatalogItemsAsOsgiWithoutStartingBundles(mgmt(), catalogYaml, new VersionedName(bundleName(), bundleVersion()), isForceUpdate());
            break;
        case USUAL_OSGI_WAY_AS_BUNDLE_WITH_DEFAULT_NAME:
            addCatalogItemsAsOsgiInUsualWay(mgmt(), catalogYaml, new VersionedName(bundleName(), bundleVersion()), isForceUpdate());
            break;
        case USUAL_OSGI_WAY_AS_ZIP_NO_MANIFEST_NAME_MAYBE_IN_BOM:
            addCatalogItemsAsOsgiInUsualWay(mgmt(), catalogYaml, null, isForceUpdate());
            break;
        }
    }

    protected String bundleName() { return "sample-bundle"; }
    protected String bundleVersion() { return BasicBrooklynCatalog.NO_VERSION; }
    
    @Override
    protected void doTestReplacementFailureLeavesPreviousIntact(boolean bundleHasId) throws Exception {
        try {
            itemsInstallMode = bundleHasId ? CatalogItemsInstallationMode.USUAL_OSGI_WAY_AS_ZIP_NO_MANIFEST_NAME_MAYBE_IN_BOM : 
                CatalogItemsInstallationMode.ADD_YAML_ITEMS_UNBUNDLED;
            super.doTestReplacementFailureLeavesPreviousIntact(bundleHasId);
        } finally {
            itemsInstallMode = null;
        }
    }
    
    @Test   // basic test that this approach to adding types works
    public void testAddTypes() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), BasicEntity.class.getName());

        Iterable<RegisteredType> itemsInstalled = mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(new VersionedName(bundleName(), bundleVersion())));
        Asserts.assertSize(itemsInstalled, 1);
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        Asserts.assertEquals(item, Iterables.getOnlyElement(itemsInstalled), "Wrong item; installed: "+itemsInstalled);
    }

    @Test // test disabled as "broken" in super but works here
    public void testSameCatalogReferences() {
        super.testSameCatalogReferences();
    }

    @Test
    public void testUpdatingItemAllowedIfEquivalentUnderRewrite() {
        String symbolicName = "my.catalog.app.id.duplicate";
        // forward reference supported here (but not in super)
        // however the plan is rewritten meaning a second install requires special comparison
        // (RegisteredTypes "equivalent plan" methods)
        addForwardReferencePlan(symbolicName);

        // delete one but not the other to prevent resolution and thus rewrite until later validation phase,
        // thus initial addition will compare unmodified plan from here against modified plan added above;
        // replacement will then succeed only if we've correctly recorded equivalence tags 
        deleteCatalogEntity("forward-referenced-entity");
        
        addForwardReferencePlan(symbolicName);
    }

    protected void addForwardReferencePlan(String symbolicName) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  items:",
            "  - id: " + symbolicName,
            "    itemType: entity",
            "    item:",
            "      type: forward-referenced-entity",
            "  - id: " + "forward-referenced-entity",
            "    itemType: entity",
            "    item:",
            "      type: " + TestEntity.class.getName());
    }
    
    // also runs many other tests from super, here using the osgi/type-registry appraoch
    
}
