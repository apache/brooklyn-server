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
package org.apache.brooklyn.camp.brooklyn.test.lite;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.spi.Assembly;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.camp.spi.pdp.PdpYamlTest;
import org.apache.brooklyn.camp.test.mock.web.MockWebPlatform;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddChildrenEffector;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/** Tests of lightweight CAMP integration. Since the "real" integration is in brooklyn-camp project,
 * but some aspects of CAMP we want to be able to test here. */
public class CampYamlLiteTest {
    private static final String TEST_VERSION = "0.1.2";

    private static final Logger log = LoggerFactory.getLogger(CampYamlLiteTest.class);
    
    protected LocalManagementContext mgmt;
    protected CampPlatformWithJustBrooklynMgmt platform;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() {
        mgmt = LocalManagementContextForTests.newInstanceWithOsgi();
        platform = new CampPlatformWithJustBrooklynMgmt(mgmt);
        MockWebPlatform.populate(platform, TestAppAssemblyInstantiator.class);
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() {
        if (mgmt!=null) mgmt.terminate();
    }
    
    /** based on {@link PdpYamlTest} for parsing,
     * then creating a {@link TestAppAssembly} */
    @Test
    public void testYamlServiceMatchAndBrooklynInstantiate() throws Exception {
        Reader input = new InputStreamReader(getClass().getResourceAsStream("test-app-service-blueprint.yaml"));
        AssemblyTemplate at = platform.pdp().registerDeploymentPlan(input);
        log.info("AT is:\n"+at.toString());
        Assert.assertEquals(at.getName(), "sample");
        Assert.assertEquals(at.getPlatformComponentTemplates().links().size(), 1);
        
        // now use brooklyn to instantiate - note it won't be faithful, but it will set some config keys
        Assembly assembly = at.getInstantiator().newInstance().instantiate(at, platform);
        
        TestApplication app = ((TestAppAssembly)assembly).getBrooklynApp();
        Assert.assertEquals( app.getConfig(TestEntity.CONF_NAME), "sample" );
        Map<String, String> map = app.getConfig(TestEntity.CONF_MAP_THING);
        Assert.assertEquals( map.get("desc"), "Tomcat sample JSP and servlet application." );
        
        Assert.assertEquals( app.getChildren().size(), 1 );
        Entity svc = Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals( svc.getConfig(TestEntity.CONF_NAME), "Hello WAR" );
        map = svc.getConfig(TestEntity.CONF_MAP_THING);
        Assert.assertEquals( map.get("type"), MockWebPlatform.APPSERVER.getType() );
        // desc ensures we got the information from the matcher, as this value is NOT in the yaml
        Assert.assertEquals( map.get("desc"), MockWebPlatform.APPSERVER.getDescription() );
    }

    /** based on {@link PdpYamlTest} for parsing,
     * then creating a {@link TestAppAssembly} */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testAddChildrenEffector() throws Exception {
        String childYaml = Streams.readFullyStringAndClose(getClass().getResourceAsStream("test-app-service-blueprint.yaml"));
        AddChildrenEffector newEff = new AddChildrenEffector(ConfigBag.newInstance()
            .configure(AddChildrenEffector.EFFECTOR_NAME, "add_tomcat")
            .configure(AddChildrenEffector.BLUEPRINT_YAML, childYaml)
            .configure(AddChildrenEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("war", (Object)MutableMap.of(
                "defaultValue", "foo.war"))) ) ;
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class).addInitializer(newEff));

        // test adding, with a parameter
        Task<List> task = app.invoke(Effectors.effector(List.class, "add_tomcat").buildAbstract(), MutableMap.of("war", "foo.bar"));
        List result = task.get();
        
        Entity newChild = Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(newChild.getConfig(ConfigKeys.newStringConfigKey("war")), "foo.bar");
        
        Assert.assertEquals(Iterables.getOnlyElement(result), newChild.getId());
        Entities.unmanage(newChild);
        
        // and test default value
        task = app.invoke(Effectors.effector(List.class, "add_tomcat").buildAbstract(), MutableMap.<String,Object>of());
        result = task.get();
        
        newChild = Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(newChild.getConfig(ConfigKeys.newStringConfigKey("war")), "foo.war");
        
        Assert.assertEquals(Iterables.getOnlyElement(result), newChild.getId());
        Entities.unmanage(newChild);
    }

    @Test
    public void testYamlServiceForCatalog() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        installWithoutCatalogBom(mgmt, OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL);

        CatalogItem<?, ?> realItem = Iterables.getOnlyElement(mgmt.getCatalog().addItems(Streams.readFullyStringAndClose(getClass().getResourceAsStream("test-app-service-blueprint.yaml"))));
        Iterable<CatalogItem<Object, Object>> retrievedItems = mgmt.getCatalog()
                .getCatalogItems(CatalogPredicates.symbolicName(Predicates.equalTo("catalog-name")));
        
        Assert.assertEquals(Iterables.size(retrievedItems), 1, "Wrong retrieved items: "+retrievedItems);
        CatalogItem<Object, Object> retrievedItem = Iterables.getOnlyElement(retrievedItems);
        Assert.assertEquals(retrievedItem, realItem);

        Collection<CatalogBundle> bundles = retrievedItem.getLibraries();
        Assert.assertEquals(bundles.size(), 1);
        CatalogBundle bundle = Iterables.getOnlyElement(bundles);
        Assert.assertEquals(bundle.getUrl(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL);
        Assert.assertEquals(bundle.getSuppliedVersionString(), "0.1.0");

        EntitySpec<?> spec1 = (EntitySpec<?>) mgmt.getCatalog().peekSpec(retrievedItem);
        assertNotNull(spec1);
        Assert.assertEquals(spec1.getConfig().get(TestEntity.CONF_NAME), "sample");
        
        // TODO other assertions, about children
    }

    @Test
    public void testRegisterCustomEntityWithBundleWhereEntityIsFromCoreAndIconFromBundle() throws IOException {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.app.id";
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String yaml = prepAndGetSampleMyCatalogAppYaml(symbolicName, bundleUrl);

        mgmt.getCatalog().addItems(yaml);

        assertMgmtHasSampleMyCatalogApp(symbolicName, bundleUrl);
    }

    private String prepAndGetSampleMyCatalogAppYaml(String symbolicName, String bundleUrl) {
        installWithoutCatalogBom(mgmt, bundleUrl);
        
        return Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  libraries:",
                "  - url: " + bundleUrl,
                "  item:",
                "    type: " + MockWebPlatform.APPSERVER.getName());
    }

    protected void installWithoutCatalogBom(LocalManagementContext mgmt, String bundleUrl) {
        // install bundle for class access but without loading its catalog.bom, 
        // since we only have mock matchers here
        // (if we don't do this, the default routines install it and try to process the catalog.bom, failing)
        mgmt.getOsgiManager().get().installDeferredStart(new BasicManagedBundle(null, null, bundleUrl), null).get();
    }

    private void assertMgmtHasSampleMyCatalogApp(String symbolicName, String bundleUrl) {
        RegisteredType item = mgmt.getTypeRegistry().get(symbolicName);
        assertNotNull(item, "failed to load item with id=" + symbolicName + " from catalog. Entries were: " +
                Joiner.on(",").join(mgmt.getTypeRegistry().getAll()));
        assertEquals(item.getSymbolicName(), symbolicName);

        RegisteredTypes.tryValidate(item, RegisteredTypeLoadingContexts.spec(Entity.class)).get();
        
        // stored as yaml, not java
        String planYaml = RegisteredTypes.getImplementationDataStringForSpec(item);
        assertNotNull(planYaml);
        Assert.assertTrue(planYaml.contains("io.camp.mock:AppServer"));

        // and let's check we have libraries
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        OsgiBundleWithUrl bundle = Iterables.getOnlyElement(libs);
        assertEquals(bundle.getUrl(), bundleUrl);

        // now let's check other things on the item
        assertEquals(item.getDisplayName(), "My Catalog App");
        assertEquals(item.getDescription(), "My description");
        assertEquals(item.getIconUrl(), "classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");

        // and confirm we can resolve ICON
        byte[] iconData = Streams.readFullyAndClose(ResourceUtils.create(CatalogUtils.newClassLoadingContext(mgmt, item)).getResourceFromUrl(item.getIconUrl()));
        assertEquals(iconData.length, 43);
    }

}
