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

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.camp.spi.PlatformRootSummary;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Joiner;

public class AbstractYamlRebindTest extends RebindTestFixture<StartableApplication> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractYamlRebindTest.class);
    protected static final String TEST_VERSION = "0.1.2";

    protected BrooklynCampPlatform platform;
    protected BrooklynCampPlatformLauncherNoServer launcher;
    private boolean forceUpdate;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        launcher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return (LocalManagementContext) mgmt();
            }
        };
        launcher.launch();
        platform = launcher.getCampPlatform();
        
        forceUpdate = false;
    }

    @Override
    protected LocalManagementContext createNewManagementContext(File mementoDir, HighAvailabilityMode haMode, Map<?, ?> additionalProperties) {
        LocalManagementContext newMgmt = super.createNewManagementContext(mementoDir, haMode, additionalProperties);
        new BrooklynCampPlatform(
                PlatformRootSummary.builder().name("Brooklyn CAMP Platform").build(),
                newMgmt)
            .setConfigKeyAtManagmentContext();
        return newMgmt;
    }
    
    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (launcher != null) launcher.stopServers();
        }
    }

    @Override
    protected StartableApplication rebind(RebindOptions options) throws Exception {
        StartableApplication result = super.rebind(options);
        if (launcher != null) {
            launcher.stopServers();
            launcher = new BrooklynCampPlatformLauncherNoServer() {
                @Override
                protected LocalManagementContext newMgmtContext() {
                    return (LocalManagementContext) mgmt();
                }
            };
            launcher.launch();
            platform = launcher.getCampPlatform();
        }
        return result;
    }

    @Override
    protected StartableApplication createApp() {
        return null;
    }

    @Override
    protected ManagementContext mgmt() {
        return (newManagementContext != null) ? newManagementContext : origManagementContext;
    }

    ///////////////////////////////////////////////////
    // TODO code below is duplicate of AbstractYamlTest
    ///////////////////////////////////////////////////

    protected void waitForApplicationTasks(Entity app) {
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), app);
        getLogger().info("Waiting on " + tasks.size() + " task(s)");
        for (Task<?> t : tasks) {
            t.blockUntilEnded();
        }
    }

    protected Reader loadYaml(String yamlFileName, String ...extraLines) throws Exception {
        return new StringReader(AbstractYamlTest.loadYaml(this, yamlFileName, extraLines));
    }

    protected String loadYamlString(String yamlFileName, String ...extraLines) throws Exception {
        return AbstractYamlTest.loadYaml(this, yamlFileName, extraLines);
    }
    
    /**
     * @deprecated since 0.11.0, use {@link #createAndStartApplication(String)} instead,
     *             in the same way as {@link AbstractYamlTest}.
     */
    @Deprecated
    protected Entity createAndStartApplication(Reader input) throws Exception {
        return createAndStartApplication(Streams.readFully(input));
    }

    protected Entity createAndStartApplication(String... multiLineYaml) throws Exception {
        return createAndStartApplication(joinLines(multiLineYaml));
    }

    protected Entity createAndStartApplication(String input) throws Exception {
        return createAndStartApplication(input, MutableMap.<String,String>of());
    }
    
    protected Entity createAndStartApplication(String input, Map<String,?> startParameters) throws Exception {
        final Entity app = createApplicationUnstarted(input);
        
        app.invoke(Startable.START, startParameters).get();
        getLogger().info("Test started app " + app);
        
        return app;
    }

    protected Entity createStartWaitAndLogApplication(String... input) throws Exception {
        return createStartWaitAndLogApplication(joinLines(input));
    }

    protected Entity createStartWaitAndLogApplication(String input) throws Exception {
        return createStartWaitAndLogApplication(new StringReader(input));
    }

    protected Entity createStartWaitAndLogApplication(Reader input) throws Exception {
        Entity app = createAndStartApplication(input);
        waitForApplicationTasks(app);

        getLogger().info("App started:");
        Dumper.dumpInfo(app);

        return app;
    }

    protected Entity createApplicationUnstarted(String... multiLineYaml) throws Exception {
        return createApplicationUnstarted(joinLines(multiLineYaml));
    }
    
    protected Entity createApplicationUnstarted(String input) throws Exception {
        // starting of the app happens automatically if we use camp to instantiate, but not if we use create spec approach.
        EntitySpec<?> spec = 
            mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, input, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = mgmt().getEntityManager().createEntity(spec);
        getLogger().info("Test created app (unstarted) " + app);
        return app;
    }
    
    protected void addCatalogItems(Iterable<String> catalogYaml) {
        addCatalogItems(joinLines(catalogYaml));
    }

    protected void addCatalogItems(String... catalogYaml) {
        addCatalogItems(joinLines(catalogYaml));
    }

    protected Iterable<? extends CatalogItem<?,?>> addCatalogItems(String catalogYaml) {
        return mgmt().getCatalog().addItems(catalogYaml);
    }

    protected void deleteCatalogEntity(String catalogItem) {
        mgmt().getCatalog().deleteCatalogItem(catalogItem, TEST_VERSION);
    }

    protected Logger getLogger() {
        return LOG;
    }

    private String joinLines(Iterable<String> catalogYaml) {
        return Joiner.on("\n").join(catalogYaml);
    }

    private String joinLines(String[] catalogYaml) {
        return Joiner.on("\n").join(catalogYaml);
    }

    protected String ver(String id) {
        return CatalogUtils.getVersionedId(id, TEST_VERSION);
    }

    public void forceCatalogUpdate() {
        forceUpdate = true;
    }

}
