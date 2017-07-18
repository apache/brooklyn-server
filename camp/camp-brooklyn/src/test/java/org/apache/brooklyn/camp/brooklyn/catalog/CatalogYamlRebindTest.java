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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoPersister;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore.StoreObjectAccessor;
import org.apache.brooklyn.core.mgmt.rebind.RebindExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.core.test.policy.TestEnricher;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class CatalogYamlRebindTest extends AbstractYamlRebindTest {

    // TODO Other tests (relating to https://issues.apache.org/jira/browse/BROOKLYN-149) include:
    //   - entities cannot be instantiated because class no longer on classpath (e.g. was OSGi)
    //   - config/attribute cannot be instantiated (e.g. because class no longer on classpath)
    //   - entity file corrupt

    // Since 0.12.0 OSGi reads from bundles so many of the things this used to test are no longer supported;
    // deprecation and disablement will have to be done on a bundle-wide basis
    // and transforms will have to be done by forcibly replacing a bundle or another "upgrade" mechanism
    
    enum RebindWithCatalogTestMode {
        NO_OP,
        STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM,
        DEPRECATE_CATALOG,
        DISABLE_CATALOG,
        DELETE_CATALOG,
        REPLACE_CATALOG_WITH_NEWER_VERSION;
    }
    
    enum OsgiMode {
        NONE,
        NORMAL
    }

    boolean useOsgi = false;
    private Boolean defaultEnablementOfFeatureAutoFixCatalogRefOnRebind;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        defaultEnablementOfFeatureAutoFixCatalogRefOnRebind = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND);
        super.setUp();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (defaultEnablementOfFeatureAutoFixCatalogRefOnRebind != null) {
            BrooklynFeatureEnablement.setEnablement(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND, defaultEnablementOfFeatureAutoFixCatalogRefOnRebind);
        }
        super.tearDown();
    }
    
    @Override
    protected boolean useOsgi() {
        return useOsgi || (origManagementContext!=null && ((ManagementContextInternal)origManagementContext).getOsgiManager().isPresent());
    }

    @DataProvider
    public Object[][] dataProvider() {
        return new Object[][] {
            {RebindWithCatalogTestMode.NO_OP, OsgiMode.NONE},
            
            {RebindWithCatalogTestMode.STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM, OsgiMode.NONE},
            
            {RebindWithCatalogTestMode.DEPRECATE_CATALOG, OsgiMode.NONE},
            
            {RebindWithCatalogTestMode.DISABLE_CATALOG, OsgiMode.NONE},
            
            // For DELETE_CATALOG, see https://issues.apache.org/jira/browse/BROOKLYN-149.
            // Deletes the catalog item before rebind, but the referenced types are still on the 
            // default classpath. Will fallback to loading from classpath.
            //
            // Does not work for OSGi, because our bundle will no longer be available.
            {RebindWithCatalogTestMode.DELETE_CATALOG, OsgiMode.NONE},
            
            // Upgrades the catalog item before rebind, deleting the old version.
            // Will automatically upgrade. Test will enable "FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND"
            {RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION, OsgiMode.NONE},
        };
    }

    @Test(dataProvider = "dataProvider")
    public void testRebindWithCatalogAndApp(RebindWithCatalogTestMode mode, OsgiMode osgiMode) throws Exception {
        testRebindWithCatalogAndAppUsingOptions(mode, osgiMode, RebindOptions.create());
    }

    // Re-run all the same tests as testRebindWithCatalogAndApp, but with the XML updated to mimic state
    // persisted before <catalogItemIdSearchPath> was introduced.
    @Test(dataProvider = "dataProvider")
    public void testRebindWithCatalogAndAppRebindCatalogItemIds(RebindWithCatalogTestMode mode, OsgiMode osgiMode) throws Exception {
        final RebindOptions rebindOptions = RebindOptions.create();
        applyCompoundStateTransformer(rebindOptions, CompoundTransformer.builder()
            .xmlDeleteItem("//searchPath") // delete searchPath element
            .xmlDeleteItem("//@*[contains(., 'searchPath')]") // delete any attributes that reference searchPath
            .build());
        testRebindWithCatalogAndAppUsingOptions(mode, osgiMode, rebindOptions);
    }

    private void applyCompoundStateTransformer(RebindOptions options, final CompoundTransformer transformer) {
        options.stateTransformer(new Function<BrooklynMementoPersister, Void>() {
                @Override public Void apply(BrooklynMementoPersister input) {

                    try {
                        BrooklynMementoRawData transformed = transformer.transform(input, RebindExceptionHandlerImpl.builder().build());
                        PersistenceObjectStore objectStore = ((BrooklynMementoPersisterToObjectStore)input).getObjectStore();
                        for (BrooklynObjectType type : BrooklynObjectType.values()) {
                            final List<String> contents = objectStore.listContentsWithSubPath(type.getSubPathName());
                            for (String path : contents) {
                                if (path.endsWith(".jar")) {
                                    // don't apply transformers to JARs
                                    continue;
                                }
                                StoreObjectAccessor accessor = objectStore.newAccessor(path);
                                String memento = checkNotNull(accessor.get(), path);
                                String replacement = transformed.getObjectsOfType(type).get(idFromPath(type, path));
                                getLogger().trace("Replacing {} with {}", memento, replacement);
                                accessor.put(replacement);
                            }
                        }
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        getLogger().warn(Strings.join(new Object[]{
                            "Caught'", e.getMessage(), "' when transforming '", input.getBackingStoreDescription()
                        }, ""), e);
                    }

                    return null;
                }});
    }

    private String idFromPath(BrooklynObjectType type, String path) {
        // the replace underscore with colon below handles file names of catalog items like "catalog/my.catalog.app.id.load_0.1.0"
        return path.substring(type.getSubPathName().length()+1).replace('_', ':');
    }


    @SuppressWarnings({ "deprecation", "unused" })
    public void testRebindWithCatalogAndAppUsingOptions(RebindWithCatalogTestMode mode, OsgiMode osgiMode, RebindOptions options) throws Exception {
        if (osgiMode != OsgiMode.NONE) {
            TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
            
            recreateOrigManagementContextWithOsgi();
        }

        if (mode == RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION) {
            BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND);
        }

        String appSymbolicName = "my.catalog.app.id.load";
        String appVersion = "0.1.0";

        String appCatalogFormat = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: " + appSymbolicName,
            "  version: %s",
            "  itemType: entity",
            "  item:",
            "    type: " + BasicEntity.class.getName(),
            "    brooklyn.enrichers:",
            "    - type: " + TestEnricher.class.getName(),
            "    brooklyn.policies:",
            "    - type: " + TestPolicy.class.getName());

        String locSymbolicName = "my.catalog.loc.id.load";
        String locVersion = "1.0.0";
        String locCatalogFormat = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: " + locSymbolicName,
            "  version: %s",
            "  itemType: location",
            "  item:",
            "    type: localhost");

        // Create the catalog items
        CatalogItem<?, ?> appItem = Iterables.getOnlyElement(addCatalogItems(String.format(appCatalogFormat, appVersion)));
        CatalogItem<?, ?> locItem = Iterables.getOnlyElement(addCatalogItems(String.format(locCatalogFormat, locVersion)));

        // Create an app, using that catalog item
        String yaml = "name: simple-app-yaml\n" +
            "location: \"brooklyn.catalog:" + CatalogUtils.getVersionedId(locSymbolicName, locVersion) + "\"\n" +
            "services: \n" +
            "- type: " + CatalogUtils.getVersionedId(appSymbolicName, appVersion);
        origApp = (StartableApplication) createAndStartApplication(yaml);
        Entity origEntity = Iterables.getOnlyElement(origApp.getChildren());
        Iterables.getOnlyElement(origEntity.policies());
        Iterables.tryFind(origEntity.enrichers(), Predicates.instanceOf(TestEnricher.class)).get();
        assertEquals(origEntity.getCatalogItemId(), appSymbolicName + ":" + appVersion);

        // Depending on test-mode, delete the catalog item, and then rebind
        switch (mode) {
            case DEPRECATE_CATALOG:
                CatalogUtils.setDeprecated(mgmt(), appSymbolicName, appVersion, true);
                CatalogUtils.setDeprecated(mgmt(), locSymbolicName, locVersion, true);
                break;
            case DISABLE_CATALOG:
                CatalogUtils.setDisabled(mgmt(), appSymbolicName, appVersion, true);
                CatalogUtils.setDisabled(mgmt(), locSymbolicName, locVersion, true);
                break;
            case DELETE_CATALOG:
                mgmt().getCatalog().deleteCatalogItem(appSymbolicName, appVersion);
                mgmt().getCatalog().deleteCatalogItem(locSymbolicName, locVersion);
                break;
            case REPLACE_CATALOG_WITH_NEWER_VERSION:
                mgmt().getCatalog().deleteCatalogItem(appSymbolicName, appVersion);
                mgmt().getCatalog().deleteCatalogItem(locSymbolicName, locVersion);
                appVersion = "0.2.0";
                locVersion = "1.1.0";
                addCatalogItems(String.format(appCatalogFormat, appVersion));
                addCatalogItems(String.format(locCatalogFormat, locVersion));
                break;
            case STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM:
                // set everything false -- then below we rebind with these fields removed to ensure that we can rebind
                CatalogUtils.setDeprecated(mgmt(), appSymbolicName, appVersion, false);
                CatalogUtils.setDeprecated(mgmt(), locSymbolicName, locVersion, false);
                CatalogUtils.setDisabled(mgmt(), appSymbolicName, appVersion, false);
                CatalogUtils.setDisabled(mgmt(), locSymbolicName, locVersion, false);
                // Edit the persisted state to remove the "deprecated" and "enablement" tags for our catalog items
                applyCompoundStateTransformer(options, CompoundTransformer.builder()
                    .xmlReplaceItem("deprecated|disabled", "")
                    .build());
                break;
            case NO_OP:
                break; // no-op
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }

        // Rebind
        rebind(options);

        // Ensure app is still there, and that it is usable - e.g. "stop" effector functions as expected
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        Iterables.getOnlyElement(newEntity.policies());
        Iterables.tryFind(newEntity.enrichers(), Predicates.instanceOf(TestEnricher.class)).get();
        assertEquals(newEntity.getCatalogItemId(), appSymbolicName + ":" + appVersion);

        newApp.stop();
        assertFalse(Entities.isManaged(newApp));
        assertFalse(Entities.isManaged(newEntity));

        // Ensure catalog item is as expecpted
        RegisteredType newAppItem = mgmt().getTypeRegistry().get(appSymbolicName, appVersion);
        RegisteredType newLocItem = mgmt().getTypeRegistry().get(locSymbolicName, locVersion);

        boolean itemDeployable;
        switch (mode) {
            case DISABLE_CATALOG:
                assertTrue(newAppItem.isDisabled());
                assertTrue(newLocItem.isDisabled());
                itemDeployable = false;
                break;
            case DELETE_CATALOG:
                assertNull(newAppItem);
                assertNull(newLocItem);
                itemDeployable = false;
                break;
            case DEPRECATE_CATALOG:
                assertTrue(newAppItem.isDeprecated());
                assertTrue(newLocItem.isDeprecated());
                itemDeployable = true;
                break;
            case NO_OP:
            case STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM:
            case REPLACE_CATALOG_WITH_NEWER_VERSION:
                assertNotNull(newAppItem);
                assertNotNull(newLocItem);
                assertFalse(newAppItem.isDeprecated());
                assertFalse(newAppItem.isDisabled());
                assertFalse(newLocItem.isDeprecated());
                assertFalse(newLocItem.isDisabled());
                itemDeployable = true;
                break;
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }

        // Try to deploy a new app
        String yaml2 = "name: simple-app-yaml2\n" +
            "location: \"brooklyn.catalog:" + CatalogUtils.getVersionedId(locSymbolicName, locVersion) + "\"\n" +
            "services: \n" +
            "- type: " + CatalogUtils.getVersionedId(appSymbolicName, appVersion);

        if (itemDeployable) {
            StartableApplication app2 = (StartableApplication) createAndStartApplication(yaml2);
            Entity entity2 = Iterables.getOnlyElement(app2.getChildren());
            assertEquals(entity2.getCatalogItemId(), appSymbolicName + ":" + appVersion);
        } else {
            try {
                StartableApplication app2 = (StartableApplication) createAndStartApplication(yaml2);
                Asserts.shouldHaveFailedPreviously("app2=" + app2);
            } catch (Exception e) {
                // only these two modes are allowed; may have different assertions (but don't yet)
                if (mode == RebindWithCatalogTestMode.DELETE_CATALOG) {
                    Asserts.expectedFailureContainsIgnoreCase(e, "unable to match", "my.catalog.app");
                } else {
                    assertEquals(mode, RebindWithCatalogTestMode.DISABLE_CATALOG);
                    Asserts.expectedFailureContainsIgnoreCase(e, "unable to match", "my.catalog.app");
                }
            }
        }
    }

    protected void recreateOrigManagementContextWithOsgi() {
        // replace with OSGi context
        Entities.destroyAll(origManagementContext);
        try {
            useOsgi = true;
            tearDown();
            setUp();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            useOsgi = false;
        }
    }
    
    @Test
    public void testLongReferenceSequenceWithoutOsgi() throws Exception {
        doTestLongReferenceSequence();
    }
    
    @Test
    public void testLongReferenceSequenceWithOsgi() throws Exception {
        recreateOrigManagementContextWithOsgi();
        doTestLongReferenceSequence();
    }
    
    private void doTestLongReferenceSequence() throws Exception {
        // adds a0, a1 extending a0, a2 extending a1, ... a9 extending a8
        // osgi rebind of types can fail because bundles are restored in any order
        // and dependencies might not yet be installed;
        // ensure items are added first without validation, then validating
        for (int i = 0; i<10; i++) {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: a" + i,
                "  version: 1",
                "  itemType: entity",
                "  item:",
                "    type: " + (i==0 ? BasicEntity.class.getName() : "a" + (i-1)));
        }
        origApp = (StartableApplication) createAndStartApplication("services: [ { type: a9 } ]");
        rebind();
        Entity child = Iterables.getOnlyElement( newApp.getChildren() );
        Asserts.assertTrue(child instanceof BasicEntity);
        Asserts.assertEquals(child.getCatalogItemId(), "a9:1");
    }

    @Test
    public void testDeleteEmptyBundleRemovedFromPersistence() throws Exception {
        recreateOrigManagementContextWithOsgi();
        
        String bom = Strings.lines(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: sample",
                "    item:",
                "      type: " + BasicEntity.class.getName());
        addCatalogItems(bom);
        addCatalogItems(bom);
        rebind();
        // should only contain one bundle / bundle.jar pair
        Asserts.assertSize(Arrays.asList( new File(mementoDir, "bundles").list() ), 2);
    }

}
