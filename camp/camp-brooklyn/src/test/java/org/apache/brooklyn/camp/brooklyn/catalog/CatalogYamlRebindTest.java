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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoPersister;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.StartableApplication;
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
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CatalogYamlRebindTest extends AbstractYamlRebindTest {

    // TODO Other tests (relating to https://issues.apache.org/jira/browse/BROOKLYN-149) include:
    //   - entities cannot be instantiated because class no longer on classpath (e.g. was OSGi)
    //   - config/attribute cannot be instantiated (e.g. because class no longer on classpath)
    //   - entity file corrupt

    private static final String OSGI_BUNDLE_SYMBOLID_NAME_FULL = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FULL;
    private static final String OSGI_BUNDLE_URL = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
    private static final String OSGI_SIMPLE_ENTITY_TYPE = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;
    private static final String OSGI_SIMPLE_POLICY_TYPE = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_POLICY;
    private static final String OSGI_SIMPLE_EFFECTOR_TYPE = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_EFFECTOR;

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
        LIBRARY,
        PREFIX
    }

    private Boolean defaultEnablementOfFeatureAutoFixatalogRefOnRebind;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        defaultEnablementOfFeatureAutoFixatalogRefOnRebind = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND);
        super.setUp();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (defaultEnablementOfFeatureAutoFixatalogRefOnRebind != null) {
            BrooklynFeatureEnablement.setEnablement(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND, defaultEnablementOfFeatureAutoFixatalogRefOnRebind);
        }
        super.tearDown();
    }
    
    @Override
    protected boolean useOsgi() {
        return true;
    }

    @DataProvider
    public Object[][] dataProvider() {
        return new Object[][] {
            {RebindWithCatalogTestMode.NO_OP, OsgiMode.NONE},
            {RebindWithCatalogTestMode.NO_OP, OsgiMode.LIBRARY},
            {RebindWithCatalogTestMode.NO_OP, OsgiMode.PREFIX},
            
            {RebindWithCatalogTestMode.STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM, OsgiMode.NONE},
            {RebindWithCatalogTestMode.STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM, OsgiMode.LIBRARY},
            {RebindWithCatalogTestMode.STRIP_DEPRECATION_AND_ENABLEMENT_FROM_CATALOG_ITEM, OsgiMode.PREFIX},
            
            {RebindWithCatalogTestMode.DEPRECATE_CATALOG, OsgiMode.NONE},
            {RebindWithCatalogTestMode.DEPRECATE_CATALOG, OsgiMode.LIBRARY},
            {RebindWithCatalogTestMode.DEPRECATE_CATALOG, OsgiMode.PREFIX},
            
            {RebindWithCatalogTestMode.DISABLE_CATALOG, OsgiMode.NONE},
            {RebindWithCatalogTestMode.DISABLE_CATALOG, OsgiMode.LIBRARY},
            {RebindWithCatalogTestMode.DISABLE_CATALOG, OsgiMode.PREFIX},
            
            // For DELETE_CATALOG, see https://issues.apache.org/jira/browse/BROOKLYN-149.
            // Deletes the catalog item before rebind, but the referenced types are still on the 
            // default classpath. Will fallback to loading from classpath.
            //
            // Does not work for OSGi, because our bundle will no longer be available.
            {RebindWithCatalogTestMode.DELETE_CATALOG, OsgiMode.NONE},
            
            // Upgrades the catalog item before rebind, deleting the old version.
            // Will automatically upgrade. Test will enable "FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND"
            {RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION, OsgiMode.NONE},
            {RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION, OsgiMode.LIBRARY},
            {RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION, OsgiMode.PREFIX},
        };
    }

    @Test(dataProvider = "dataProvider")
    public void testRebindWithCatalogAndApp(RebindWithCatalogTestMode mode, OsgiMode osgiMode) throws Exception {
        testRebindWithCatalogAndAppUsingOptions(mode, osgiMode, RebindOptions.create());
    }

    // Re-run the same tests as testRebindWithCatalogAndApp but with the XML updated to mimic state
    // persisted before <catalogItemId> was replaced with <catalogItemHierarchy>.
    @Test(dataProvider = "dataProvider")
    public void testRebindWithCatalogAndAppRebindCatalogItemIds(RebindWithCatalogTestMode mode, OsgiMode osgiMode) throws Exception {
        final RebindOptions rebindOptions = RebindOptions.create();
        applyCompoundStateTransformer(rebindOptions, CompoundTransformer.builder()
            .xmlReplaceItem("//catalogItemHierarchy", "<catalogItemId><xsl:value-of select=\"string\"/></catalogItemId>")
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
        }

        if (mode == RebindWithCatalogTestMode.REPLACE_CATALOG_WITH_NEWER_VERSION) {
            BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_AUTO_FIX_CATALOG_REF_ON_REBIND);
        }

        String appSymbolicName = "my.catalog.app.id.load";
        String appVersion = "0.1.0";

        String appCatalogFormat;
        if (osgiMode == OsgiMode.LIBRARY) {
            appCatalogFormat = Joiner.on("\n").join(
                    "brooklyn.catalog:",
                    "  id: " + appSymbolicName,
                    "  version: %s",
                    "  itemType: entity",
                    "  libraries:",
                    "  - url: " + OSGI_BUNDLE_URL,
                    "  item:",
                    "    type: " + OSGI_SIMPLE_ENTITY_TYPE,
                    "    brooklyn.enrichers:",
                    "    - type: " + TestEnricher.class.getName(),
                    "    brooklyn.policies:",
                    "    - type: " + OSGI_SIMPLE_POLICY_TYPE,
                    "    brooklyn.initializers:",
                    "    - type: " + OSGI_SIMPLE_EFFECTOR_TYPE);
        } else if (osgiMode == OsgiMode.PREFIX) {
            // This catalog item is just meant to load the bundle in the OSGi environment. Its content is irrelevant.
            String libraryItem = Joiner.on("\n").join(
                    "brooklyn.catalog:",
                    "  id: dummy",
                    "  version: %s",
                    "  itemType: entity",
                    "  libraries:",
                    "  - url: " + OSGI_BUNDLE_URL,
                    "  item: " + BasicEntity.class.getName());
            addCatalogItems(String.format(libraryItem, appVersion));

            // Use bundle prefixes here, pointing to the bundle already loaded above
            appCatalogFormat = Joiner.on("\n").join(
                    "brooklyn.catalog:",
                    "  id: " + appSymbolicName,
                    "  version: %s",
                    "  itemType: entity",
                    "  item:",
                    "    type: " + OSGI_BUNDLE_SYMBOLID_NAME_FULL + ":" + OSGI_SIMPLE_ENTITY_TYPE,
                    "    brooklyn.enrichers:",
                    "    - type: " + TestEnricher.class.getName(),
                    "    brooklyn.policies:",
                    "    - type: " + OSGI_BUNDLE_SYMBOLID_NAME_FULL + ":" + OSGI_SIMPLE_POLICY_TYPE,
                    "    brooklyn.initializers:",
                    "    - type: " + OSGI_BUNDLE_SYMBOLID_NAME_FULL + ":" + OSGI_SIMPLE_EFFECTOR_TYPE);
        } else {
            appCatalogFormat = Joiner.on("\n").join(
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
        }


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
}
