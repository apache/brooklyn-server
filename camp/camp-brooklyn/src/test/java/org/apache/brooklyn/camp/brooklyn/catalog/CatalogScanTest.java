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

import java.net.URLEncoder;
import java.util.List;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatformLauncherNoServer;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.MyCatalogItems.MySillyAppTemplate;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class CatalogScanTest {

    // TODO setUp/tearDown copied from AbstractYamlTest
    
    // Moved from brooklyn-core. When we deleted support for catalog.xml, the scanned catalog
    // was then only stored in camp format, which meant we needed the camp parser.

    private static final Logger log = LoggerFactory.getLogger(CatalogScanTest.class);

    private BrooklynCatalog defaultCatalog, annotsCatalog, fullCatalog;
    
    private List<LocalManagementContext> mgmts = Lists.newCopyOnWriteArrayList();
    private List<BrooklynCampPlatformLauncherNoServer> launchers = Lists.newCopyOnWriteArrayList();

    /** Override to enable OSGi in the management context for all tests in the class. */
    protected boolean disableOsgi() {
        return true;
    }
    
    protected boolean useDefaultProperties() {
        return false;
    }
    
    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        for (LocalManagementContext mgmt : mgmts) {
            Entities.destroyAll(mgmt);
        }
        mgmts.clear();
        
        for (BrooklynCampPlatformLauncherNoServer launcher : launchers) {
            launcher.stopServers();
        }
        launchers.clear();
    }

    private LocalManagementContext newManagementContext(BrooklynProperties props) {
        final LocalManagementContext localMgmt = LocalManagementContextForTests.builder(true)
                .disableOsgi(disableOsgi())
                .useProperties(props)
                .build();
        mgmts.add(localMgmt);
        
        BrooklynCampPlatformLauncherNoServer launcher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return localMgmt;
            }
        };
        launchers.add(launcher);
        launcher.launch();
        
        return localMgmt;
    }
    
    private synchronized void loadTheDefaultCatalog(boolean lookOnDiskForDefaultCatalog) {
        if (defaultCatalog!=null) return;
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put(BrooklynServerConfig.BROOKLYN_CATALOG_URL.getName(),
            // if default catalog is picked up from the system, we might get random stuff from ~/.brooklyn/ instead of the default;
            // useful as an integration check that we default correctly, but irritating for people to use if they have such a catalog installed
            (lookOnDiskForDefaultCatalog ? "" :
                "data:,"+Urls.encode(new ResourceUtils(this).getResourceAsString("classpath:/brooklyn/default.catalog.bom"))));
        LocalManagementContext mgmt = newManagementContext(props);
        defaultCatalog = mgmt.getCatalog();
        log.info("ENTITIES loaded for DEFAULT: "+defaultCatalog.getCatalogItems(Predicates.alwaysTrue()));
    }
    
    @SuppressWarnings("deprecation")
    private synchronized void loadAnnotationsOnlyCatalog() {
        if (annotsCatalog!=null) return;
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put(BrooklynServerConfig.BROOKLYN_CATALOG_URL.getName(),
                "data:,"+URLEncoder.encode("brooklyn.catalog: {scanJavaAnnotations: true}"));
        LocalManagementContext mgmt = newManagementContext(props);
        annotsCatalog = mgmt.getCatalog();
        log.info("ENTITIES loaded with annotation: "+annotsCatalog.getCatalogItems(Predicates.alwaysTrue()));
    }
    
    @Test
    public void testLoadAnnotations() {
        loadAnnotationsOnlyCatalog();
        BrooklynCatalog c = annotsCatalog;
        
        Iterable<CatalogItem<Object,Object>> bases = c.getCatalogItems(CatalogPredicates.displayName(Predicates.containsPattern("MyBaseEntity")));
        Assert.assertEquals(Iterables.size(bases), 0, "should have been empty: "+bases);
        
        Iterable<CatalogItem<Object,Object>> asdfjkls = c.getCatalogItems(CatalogPredicates.displayName(Predicates.containsPattern("__asdfjkls__shouldnotbefound")));
        Assert.assertEquals(Iterables.size(asdfjkls), 0);
        
        Iterable<CatalogItem<Object,Object>> silly1 = c.getCatalogItems(CatalogPredicates.displayName(Predicates.equalTo("MySillyAppTemplate")));
        CatalogItem<Object, Object> silly1El = Iterables.getOnlyElement(silly1);
        
        CatalogItem<Application,EntitySpec<? extends Application>> s1 = c.getCatalogItem(Application.class, silly1El.getSymbolicName(), silly1El.getVersion());
        Assert.assertEquals(s1, silly1El);
        
        Assert.assertEquals(s1.getDescription(), "Some silly app test");
        
        Class<?> app = ((EntitySpec<?>)c.peekSpec(s1)).getImplementation();
        Assert.assertEquals(app, MySillyAppTemplate.class);
    }

    @Test
    public void testAnnotationLoadsSomeApps() {
        loadAnnotationsOnlyCatalog();
        Iterable<CatalogItem<Object,Object>> silly1 = annotsCatalog.getCatalogItems(CatalogPredicates.displayName(Predicates.equalTo("MySillyAppTemplate")));
        Assert.assertEquals(Iterables.getOnlyElement(silly1).getDescription(), "Some silly app test");
    }
    
    @Test
    public void testAnnotationLoadsSomeAppBuilders() {
        loadAnnotationsOnlyCatalog();
        Iterable<CatalogItem<Object,Object>> silly1 = annotsCatalog.getCatalogItems(CatalogPredicates.displayName(Predicates.equalTo("MySillyAppBuilderTemplate")));
        Assert.assertEquals(Iterables.getOnlyElement(silly1).getDescription(), "Some silly app builder test");
    }
    
    @Test
    public void testAnnotationIsDefault() {
        doTestAnnotationIsDefault(false);
    }

    // see comment in load method; likely fails if a custom catalog is installed in ~/.brooklyn/
    @Test(groups="Integration", enabled=false)
    public void testAnnotationIsDefaultOnDisk() {
        doTestAnnotationIsDefault(true);
    }

    private void doTestAnnotationIsDefault(boolean lookOnDiskForDefaultCatalog) {
        loadTheDefaultCatalog(lookOnDiskForDefaultCatalog);
        Iterable<CatalogItem<Object, Object>> defaults = defaultCatalog.getCatalogItems(Predicates.alwaysTrue());
        int numInDefault = Iterables.size(defaults);
        
        loadAnnotationsOnlyCatalog();
        Iterable<CatalogItem<Object, Object>> annotatedItems = annotsCatalog.getCatalogItems(Predicates.alwaysTrue());
        int numFromAnnots = Iterables.size(annotatedItems);
        
        Assert.assertEquals(numInDefault, numFromAnnots, "defaults="+defaults+"; annotatedItems="+annotatedItems);
        Assert.assertTrue(numInDefault>0, "Expected more than 0 entries");
    }

    // a simple test asserting no errors when listing the real catalog, and listing them for reference
    // also useful to test variants in a stored catalog to assert they all load
    // TODO integration tests which build up catalogs assuming other things are installed
    @Test
    public void testListCurrentCatalogItems() {
        LocalManagementContext mgmt = newManagementContext(BrooklynProperties.Factory.newDefault());
        log.info("ITEMS\n"+Strings.join(mgmt.getCatalog().getCatalogItems(), "\n"));
    }

}
