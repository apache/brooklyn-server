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
package org.apache.brooklyn.launcher.blueprints;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatform;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatformLauncherAbstract;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatformLauncherNoServer;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.camp.spi.PlatformRootSummary;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.launcher.BrooklynViewerLauncher;
import org.apache.brooklyn.launcher.SimpleYamlLauncherForTests;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/** Ancestor for blueprint tests which can easily allow rebind (default true) and a REST API attached (default false). */
public abstract class AbstractBlueprintTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBlueprintTest.class);
    
    protected File mementoDir;
    protected ClassLoader classLoader = AbstractBlueprintTest.class.getClassLoader();
    
    protected ManagementContext mgmt;
    protected SimpleYamlLauncherForTests launcher;
    protected Set<BrooklynViewerLauncher> viewers = MutableSet.of();

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        mementoDir = Os.newTempDir(getClass());
        mgmt = createOrigManagementContext();

        // required for REST access - otherwise it is viewed as not yet ready
        ((RebindManagerImpl)mgmt.getRebindManager()).setAwaitingInitialRebind(false);

        if (isRebindEnabled()) {
            LOG.info("Test " + getClass() + " persisting to " + mementoDir);
        }

        startViewer(true);

        launcher = new SimpleYamlLauncherForTests() {
            @Override
            protected BrooklynCampPlatformLauncherAbstract newPlatformLauncher() {
                return new BrooklynCampPlatformLauncherNoServer() {
                    @Override
                    public BrooklynCampPlatformLauncherAbstract launch() {
                        useManagementContext(AbstractBlueprintTest.this.mgmt);
                        return super.launch();
                    }
                };
            }
        };
    }

    /** starts a REST API to explore details; a UI can be created pointing at this API to view details graphically.
     * typical usage is for a thread-only breakpoint to be set at the point in the test where you want to explore. */
    protected void startViewer(boolean killCurrent) {
        if (isViewerEnabled()) {
            if (killCurrent) {
                // typically we kill the old and restart on the same port during rebind;
                // the old mgmt context is no longer active so isn't useful;
                // but if we wanted to have multiple viewers we could
                stopAllViewers();
            }

            BrooklynViewerLauncher viewer = BrooklynViewerLauncher.newInstance();
            synchronized (viewers) {
                viewers.add(viewer);
            }

            viewer.managementContext(mgmt);

            // other persistence options come from mgmt console but launcher needs to know this:
            viewer.persistMode(PersistMode.AUTO);

            viewer.start();
        }
    }

    protected void stopAllViewers() {
        synchronized (viewers) {
            viewers.forEach(BrooklynViewerLauncher::terminate);
            viewers.clear();
        }
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            if (mgmt != null) {
                for (Application app: mgmt.getApplications()) {
                    LOG.debug("destroying app "+app+" (managed? "+Entities.isManaged(app)+"; mgmt is "+mgmt+")");
                    try {
                        Entities.destroy(app, true);
                        LOG.debug("destroyed app "+app+"; mgmt now "+mgmt);
                    } catch (Exception e) {
                        LOG.error("problems destroying app "+app, e);
                    }
                }
            }
            if (launcher != null) launcher.destroyAll();
            if (viewers!=null) stopAllViewers();
            if (mgmt != null) Entities.destroyAll(mgmt);
            if (mementoDir != null) FileBasedObjectStore.deleteCompletely(mementoDir);
        } catch (Throwable t) {
            LOG.error("Caught exception in tearDown method", t);
        } finally {
            mgmt = null;
            launcher = null;
        }
    }

    protected void runCatalogTest(String catalogFile, Reader yamlApp) throws Exception {
        runCatalogTest(catalogFile, yamlApp, Predicates.alwaysTrue());
    }
    
    protected void runCatalogTest(String catalogFile, Reader yamlApp, Predicate<? super Application> assertion) throws Exception {
        Reader catalogInput = Streams.reader(new ResourceUtils(this).getResourceFromUrl(catalogFile));
        String catalogContent = Streams.readFullyAndClose(catalogInput);
        Iterable<? extends CatalogItem<?, ?>> items = launcher.getManagementContext().getCatalog().addItems(catalogContent);
        
        try {
            final Application app = launcher.launchAppYaml(yamlApp);
            
            assertNoFires(app);
            assertTrue(assertion.apply(app));
            
            Application newApp = rebind();
            assertNoFires(newApp);
            assertTrue(assertion.apply(app));
            
        } finally {
            for (CatalogItem<?, ?> item : items) {
                launcher.getManagementContext().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
            }
        }
    }
    
    protected Application runTest(String yamlFile) throws Exception {
        return runTestOnFile(yamlFile);
    }

    protected Application runTestOnFile(String yamlFile) throws Exception {
        return runTest(launcher.launchAppYaml(yamlFile));
    }

    protected Application runTestOnBlueprint(String blueprint) throws Exception {
        return runTest(launcher.launchAppYaml(new StringReader(blueprint)));
    }

    protected Application runTest(Application app) throws Exception {
        return runTest(app, this::assertNoFires);
    }

    protected Application runTest(Application app, Consumer<Application> check) throws Exception {
        check.accept(app);

        if (isRebindEnabled()) {
            Application newApp = rebind();
            check.accept(newApp);
            return newApp;
        }

        return app;
    }
    
    protected void runTest(Reader yaml) throws Exception {
        runTest(launcher.launchAppYaml(yaml), this::assertNoFires);
    }
    
    protected void assertNoFires(final Entity app) {
        EntityAsserts.assertAttributeEqualsEventually(app, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                for (Entity entity : Entities.descendantsAndSelf(app)) {
                    assertNotEquals(entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL), Lifecycle.ON_FIRE);
                    assertNotEquals(entity.getAttribute(Attributes.SERVICE_UP), false);
                    
                    if (entity instanceof SoftwareProcess) {
                        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
                        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_UP, Boolean.TRUE);
                    }
                }
            }});
    }

    protected Reader loadYaml(String url, String location) {
        String yaml = 
                "location: "+location+"\n"+
                new ResourceUtils(this).getResourceAsString(url);
        return new StringReader(yaml);
    }
    
    
    //////////////////////////////////////////////////////////////////
    // FOR REBIND                                                   //
    // See brooklyn.entity.rebind.RebindTestFixture in core's tests //
    //////////////////////////////////////////////////////////////////

    /** rebinds, and sets newApp */
    protected Application rebind() throws Exception {
        return rebind(RebindOptions.create());
    }

    protected Application rebind(RebindOptions options) throws Exception {
        if (!isRebindEnabled()) {
            throw new IllegalStateException("Rebind not enabled for this test; override isRebindEnabled");
        }

        ManagementContext origMgmt = mgmt;
        ManagementContext newMgmt = createNewManagementContext();
        boolean origRunning = origMgmt != null && origMgmt.isRunning();
        Collection<Application> origApps = origRunning ? origMgmt.getApplications() : Collections.emptyList();

        options = RebindOptions.create(options);
        if (options.classLoader == null) options.classLoader(classLoader);
        if (options.mementoDir == null) options.mementoDir(mementoDir);
        if (options.origManagementContext == null && origRunning) options.origManagementContext(origMgmt);
        if (options.newManagementContext == null) options.newManagementContext(newMgmt);

        for (Application origApp : origApps) {
            RebindTestUtils.stopPersistence(origApp);
        }
        
        mgmt = options.newManagementContext;

        startViewer(!isUsingNewViewerForRebind());

        Application newApp = RebindTestUtils.rebind(options);
        return newApp;
    }

    /** override this to specify whether you want a viewer created (for testing) */
    protected boolean isViewerEnabled() {
        return true;
    }

    /** override this to return true if you want separate viewers for pre- and post- rebind */
    protected boolean isUsingNewViewerForRebind() {
        return false;
    }

    protected boolean isOsgiEnabled() {
        return false;
    }

    protected RebindTestUtils.ManagementContextBuilder createBuilderForRebindingManagementContext() {
        RebindTestUtils.ManagementContextBuilder builder = RebindTestUtils.managementContextBuilder(this.mementoDir, this.classLoader)
                .persistPeriodMillis(1L)
                .forLive(true)
                .emptyCatalog(true);
        if (isOsgiEnabled()) builder.enableOsgiReusable();
        return builder;
    }

    protected LocalManagementContextForTests.Builder createBuilderForNonRebindingManagementContext() {
        LocalManagementContextForTests.Builder builder = LocalManagementContextForTests.builder(true);
        if (isOsgiEnabled()) builder.enableOsgiReusable();
        return builder;
    }

    protected LocalManagementContext createRebindingManagementContext() {
        return createBuilderForRebindingManagementContext().buildUnstarted();
    }

    protected ManagementContext createNonRebindingManagementContext() {
        LocalManagementContext mgmt = createBuilderForNonRebindingManagementContext().build();
        mgmt.getHighAvailabilityManager().disabled(false);
        return decorateManagementContext(mgmt);
    }

    /** @return An unstarted management context */
    protected ManagementContext createNewManagementContext() {
        ManagementContext newMgmt;
        if (isRebindEnabled()) {
            newMgmt = createRebindingManagementContext();
        } else {
            newMgmt = createNonRebindingManagementContext();
        }

        // add camp, for consistency with orig mgmt context
        new BrooklynCampPlatform(
                PlatformRootSummary.builder().name("Brooklyn CAMP Platform").build(),
                newMgmt)
                .setConfigKeyAtManagmentContext();

        return decorateManagementContext(newMgmt);
    }

    // -----


    protected boolean isRebindEnabled() {
        return true;
    }

    /** @return A started management context -- with or without rebind depending on the value of {@link #isRebindEnabled()} */
    protected ManagementContext createOrigManagementContext() {
        if (isRebindEnabled()) {
            return decorateManagementContext(createBuilderForRebindingManagementContext().buildStarted());
        } else {
            return createNonRebindingManagementContext();
        }
    }

    protected ManagementContext decorateManagementContext(ManagementContext mgmt) {
        return mgmt;
    }

    protected Application createAndStartApplication(String input) throws Exception {
        Application app = this.createApplicationUnstarted(input);
        app.invoke(Startable.START, MutableMap.of()).get();
        return app;
    }

    protected Application createApplicationUnstarted(String yaml) throws Exception {
        EntitySpec<Application> spec = this.createAppEntitySpec(yaml);
        Entity app = this.mgmt.getEntityManager().createEntity(spec);
        return (Application)app;
    }

    protected <T extends Application> EntitySpec<T> createAppEntitySpec(String yaml) {
        return (EntitySpec) EntityManagementUtils.createEntitySpecForApplication(this.mgmt, CampTypePlanTransformer.FORMAT, yaml);
    }

    protected void addCatalogItems(String catalogYaml) {
        mgmt.getCatalog().addTypesAndValidateAllowInconsistent(catalogYaml, (Map)null, false);
    }

    /** read the given item from the classpath, relative to this class */
    protected String read(String filenameOnClasspath) {
        // first try relative
        try {
            if (!filenameOnClasspath.startsWith("/")) {
                String absolute = Strings.replaceAllNonRegex(getClass().getPackage().getName(), ".", "/") + "/" + filenameOnClasspath;
                return ResourceUtils.create(this).getResourceAsString("classpath:"+"/"+absolute);
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // otherwise ignore, try as non-relative
        }

        return ResourceUtils.create(this).getResourceAsString("classpath:"+filenameOnClasspath);
    }

}
