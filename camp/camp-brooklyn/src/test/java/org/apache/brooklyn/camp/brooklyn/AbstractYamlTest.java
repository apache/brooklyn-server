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
package org.apache.brooklyn.camp.brooklyn;

import java.io.Reader;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests.Builder;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public abstract class AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractYamlTest.class);
    protected static final String TEST_VERSION = "0.1.2";

    private ManagementContext brooklynMgmt;
    protected BrooklynCatalog catalog;
    protected BrooklynCampPlatform platform;
    protected BrooklynCampPlatformLauncherNoServer launcher;
    private boolean forceUpdate;
    
    public AbstractYamlTest() {
        super();
    }

    protected ManagementContext mgmt() { return brooklynMgmt; }
    
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        forceUpdate = false;
        brooklynMgmt = setUpPlatform();
        catalog = brooklynMgmt.getCatalog();
    }

    protected ManagementContext setUpPlatform() {
        launcher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return newTestManagementContext();
            }
        };
        launcher.launch();
        platform = launcher.getCampPlatform();
        return launcher.getBrooklynMgmt();
    }
    
    protected LocalManagementContext newTestManagementContext() {
        Builder builder = LocalManagementContextForTests.builder(true).
            setOsgiEnablementAndReuse(!disableOsgi(), !disallowOsgiReuse());
        if (useDefaultProperties()) {
            builder.useDefaultProperties();
        }
        return builder.build();
    }

    /** Override to enable OSGi in the management context for all tests in the class. */
    protected boolean disableOsgi() {
        return true;
    }

    /** Override to disable OSGi reuse */
    protected boolean disallowOsgiReuse() {
        return false;
    }
    
    protected boolean useDefaultProperties() {
        return false;
    }
    
    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (brooklynMgmt != null) Entities.destroyAll(brooklynMgmt);
        tearDownPlatform();
    }

    protected void tearDownPlatform() throws Exception {
        if (launcher != null) launcher.stopServers();
    }

    protected void waitForApplicationTasks(Entity app) {
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(brooklynMgmt.getExecutionManager(), app);
        getLogger().info("Waiting on " + tasks.size() + " task(s)");
        for (Task<?> t : tasks) {
            t.blockUntilEnded();
        }
    }

    protected String loadYaml(String yamlFileName, String ...extraLines) throws Exception {
        ResourceUtils ru = new ResourceUtils(this);
        if (!ru.doesUrlExist(yamlFileName)) {
            if (ru.doesUrlExist(Urls.mergePaths(getClass().getPackage().getName().replace('.', '/'), yamlFileName))) {
                // look in package-specific folder if not found at root
                yamlFileName = Urls.mergePaths(getClass().getPackage().getName().replace('.', '/'), yamlFileName);
            }
        }
        String input = ru.getResourceAsString(yamlFileName).trim();
        StringBuilder builder = new StringBuilder(input);
        for (String l: extraLines)
            builder.append("\n").append(l);
        return builder.toString();
    }
    
    protected Entity createAndStartApplication(String... multiLineYaml) throws Exception {
        return createAndStartApplication(joinLines(multiLineYaml));
    }
    
    /** @deprecated since 0.10.0, use {@link #createAndStartApplication(String)} instead */
    @Deprecated
    protected Entity createAndStartApplication(Reader input) throws Exception {
        return createAndStartApplication(Streams.readFully(input));
    }

    protected Entity createAndStartApplication(String input) throws Exception {
        return createAndStartApplication(input, MutableMap.<String,String>of());
    }
    protected Entity createAndStartApplication(String input, Map<String,?> startParameters) throws Exception {
        EntitySpec<?> spec = 
            mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, input, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = brooklynMgmt.getEntityManager().createEntity(spec);
        // start the app (happens automatically if we use camp to instantiate, but not if we use crate spec approach)
        app.invoke(Startable.START, startParameters).get();
        return app;
    }

    protected Entity createAndStartApplicationAsync(String... multiLineYaml) throws Exception {
        return createAndStartApplicationAsync(joinLines(multiLineYaml));
    }

    protected Entity createAndStartApplicationAsync(String yaml) throws Exception {
        return createAndStartApplicationAsync(yaml, MutableMap.<String,String>of());
    }
    
    protected Entity createAndStartApplicationAsync(String yaml, Map<String,?> startParameters) throws Exception {
        EntitySpec<?> spec = 
            mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = brooklynMgmt.getEntityManager().createEntity(spec);
        // start the app (happens automatically if we use camp to instantiate, but not if we use create spec approach).
        // Note calling .get() on task, so this is non-blocking.
        app.invoke(Startable.START, startParameters);
        return app;
    }

    /** @deprecated since 0.10.0, use {@link #createStartWaitAndLogApplication(String)} instead */
    @Deprecated
    protected Entity createStartWaitAndLogApplication(Reader input) throws Exception {
        return createStartWaitAndLogApplication(Streams.readFully(input));
    }

    protected Entity createStartWaitAndLogApplication(String... input) throws Exception {
        return createStartWaitAndLogApplication(joinLines(input));
    }
    
    protected Entity createStartWaitAndLogApplication(String input) throws Exception {
        Entity app = createAndStartApplication(input);
        waitForApplicationTasks(app);
        getLogger().info("App started: "+app);
        return app;
    }

    protected EntitySpec<?> createAppEntitySpec(String... yaml) {
        return EntityManagementUtils.createEntitySpecForApplication(mgmt(), joinLines(yaml));
    }

    protected void addCatalogItems(Iterable<String> catalogYaml) {
        addCatalogItems(joinLines(catalogYaml));
    }

    protected void addCatalogItems(String... catalogYaml) {
        addCatalogItems(joinLines(catalogYaml));
    }

    protected void addCatalogItems(String catalogYaml) {
        mgmt().getCatalog().addItems(catalogYaml, forceUpdate);
    }

    protected void deleteCatalogEntity(String catalogItem) {
        mgmt().getCatalog().deleteCatalogItem(catalogItem, TEST_VERSION);
    }

    protected Logger getLogger() {
        return LOG;
    }

    protected String joinLines(Iterable<String> catalogYaml) {
        return Joiner.on("\n").join(catalogYaml);
    }

    protected String joinLines(String... catalogYaml) {
        return Joiner.on("\n").join(catalogYaml);
    }

    protected String ver(String id) {
        return CatalogUtils.getVersionedId(id, TEST_VERSION);
    }

    protected String ver(String id, String version) {
        return CatalogUtils.getVersionedId(id, version);
    }
    
    protected int countCatalogLocations() {
        return countCatalogItemsMatching(RegisteredTypePredicates.IS_LOCATION);
    }

    protected int countCatalogPolicies() {
        return countCatalogItemsMatching(RegisteredTypePredicates.IS_POLICY);
    }

    protected int countCatalogItemsMatching(Predicate<? super RegisteredType> filter) {
        return Iterables.size(mgmt().getTypeRegistry().getMatching(filter));
    }
    
    public void forceCatalogUpdate() {
        forceUpdate = true;
    }
}
