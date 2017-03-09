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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.camp.brooklyn.catalog.SpecParameterUnwrappingTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.ConstraintViolationException;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.internal.ssh.ExecCmdAsserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ConfigParametersYamlTest extends AbstractYamlRebindTest {
	
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParametersYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingSshTool.clear();
        }
    }
    
    @Test
    public void testConfigParameterWithOverriddenValueListedInType() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: testConfigParametersListedInType.mykey",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        testConfigParametersListedInType.mykey: myOverridingVal");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        assertKeyEquals(entity, "testConfigParametersListedInType.mykey", "myDescription", String.class, "myDefaultVal", "myOverridingVal");

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "testConfigParametersListedInType.mykey", "myDescription", String.class, "myDefaultVal", "myOverridingVal");
    }
    
    @Test
    public void testConfigParameterOverridingJavaListedInType() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: " + TestEntity.CONF_NAME.getName(),
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultYamlVal");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        assertKeyEquals(entity, TestEntity.CONF_NAME.getName(), "myDescription", String.class, "myDefaultYamlVal", "myDefaultYamlVal");

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, TestEntity.CONF_NAME.getName(), "myDescription", String.class, "myDefaultYamlVal", "myDefaultYamlVal");
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-345, and the breakage that 
    // fix originally caused - discussed in https://github.com/apache/brooklyn-server/pull/440.
    //
    // When brooklyn.parameters defines TestEntity.CONF_MAP_THING, it now means that this redefined
    // config key is used for lookup. This now has type `BasicConfigKey<Map>` rather than 
    // `MapConfigKey`. However, when the data was being written (via `entity.config().set(key, val)`)
    // it was using the `MapConfigKey`. Unfortunately the `MapConfigKey` uses a different structure
    // for storing its data (flattening out the map). So the subsequent lookup failed.
    //
    // There are three parts to fixing this:
    //  1. [DONE] In `entity.config().set(key, val)`, replace `key` with the entity's own key  
    //     (i.e. the same logic that will subsequently be used in the `get`).
    //  2. [DONE] In `BrooklynComponentTemplateResolver.findAllFlagsAndConfigKeyValues`, respect  
    //     the precedence of the config keys - prefer the `brooklyn.parameters` over the key defined
    //     in the super-type (e.g. in the java class).
    //  3. [TODO] Investigate rebind: the entity's ownConfig ends up with the "test.confMapThing.mykey=myval",
    //     so it has populated it using the MayConfigKey structure rather than the override config key.
    //  4. [TODO] Major overhaul of the ConfigKey name versus `SetFromFlag` alias. It is currently
    //     confusing in when reading the config values what the precedence is because there are 
    //     different names that are only understood by some things.
    @Test(groups="Broken")
    public void testConfigParameterOverridingJavaMapConfigKey() throws Exception {
        runConfigParameterOverridingJavaMapConfigKey(true);
    }
    
    @Test
    public void testConfigParameterOverridingJavaMapConfigKeyWithoutRebindValueCheck() throws Exception {
        // A cut-down test of what is actually working just now (so we can detect any 
        // further regressions!)
        runConfigParameterOverridingJavaMapConfigKey(false);
    }
    
    protected void runConfigParameterOverridingJavaMapConfigKey(boolean assertReboundVal) throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: " + TestEntity.CONF_MAP_THING.getName(),
                "        description: myDescription",
                "        type: java.util.Map",
                "      brooklyn.config:",
                "        "+TestEntity.CONF_MAP_THING.getName()+":",
                "          mykey: myval");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        assertKeyEquals(entity, TestEntity.CONF_MAP_THING.getName(), "myDescription", java.util.Map.class, null, ImmutableMap.of("mykey", "myval"));

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        if (assertReboundVal) {
            assertKeyEquals(newEntity, TestEntity.CONF_MAP_THING.getName(), "myDescription", java.util.Map.class, null, ImmutableMap.of("mykey", "myval"));
        } else {
            // TODO delete duplication from `assertKeyEquals`, when the above works!
            ConfigKey<?> key = newEntity.getEntityType().getConfigKey(TestEntity.CONF_MAP_THING.getName());
            assertEquals(key.getDescription(), "myDescription");
            assertEquals(key.getType(), java.util.Map.class);
            assertEquals(key.getDefaultValue(), null);
        }
    }
    
    @Test
    public void testConfigParametersListedInType() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: testConfigParametersListedInType.mykey",
                "        description: myDescription",
                "        type: java.util.Map",
                "        inheritance.type: deep_merge",
                "        default: {myDefaultKey: myDefaultVal}");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        Map<?,?> expectedVal = ImmutableMap.of("myDefaultKey", "myDefaultVal");
        assertKeyEquals(entity, "testConfigParametersListedInType.mykey", "myDescription", Map.class, expectedVal, expectedVal);

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "testConfigParametersListedInType.mykey", "myDescription", Map.class, expectedVal, expectedVal);
    }
    
    /**
     * See comment in testConfigParametersAtRootListedInTemplateSingleEntity for why we have two. 
     * Note that (surprisingly!) it's very important that there are two entities listed under 
     * "services". If there is just one, then the BasicApplication created to wrap it will not 
     * have the key. Instead, the single child will have the key. This is because the top-level 
     * app is considered "uninteresting" as it is only there to wrap a non-app entity.
     * 
     * @see {@link #testConfigParametersAtRootListedInTemplateSingleEntity()}
     */
    @Test
    public void testConfigParametersAtRootListedInTemplateApp() throws Exception {
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: template",
                "  items:",
                "  - id: template-with-top-level-params",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: test.parameter",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultParamVal",
                "      services:",
                "      - type: "+TestEntity.class.getName(),
                "      - type: "+TestEntity.class.getName()
        );
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: template-with-top-level-params");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        
        assertKeyEquals(app, "test.parameter", "myDescription", String.class, "myDefaultParamVal", "myDefaultParamVal");

        // After rebind, check config key is listed
        newApp = rebind();
        assertKeyEquals(newApp, "test.parameter", "myDescription", String.class, "myDefaultParamVal", "myDefaultParamVal");
    }

    /**
     * See comment in {@link #testConfigParametersAtRootListedInTemplateApp()} for why the key
     * is on the child entity rather than the top-level app!
     */
    @Test
    public void testConfigParametersAtRootListedInTemplateSingleEntity() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: template",
                "  items:",
                "  - id: template-with-top-level-params",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: test.parameter",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultParamVal",
                "      services:",
                "      - type: "+TestEntity.class.getName()
        );
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: template-with-top-level-params");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        
        assertKeyEquals(entity, "test.parameter", "myDescription", String.class, "myDefaultParamVal", "myDefaultParamVal");
        
        // After rebind, check config key is listed
        newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "test.parameter", "myDescription", String.class, "myDefaultParamVal", "myDefaultParamVal");
    }

    @Test
    public void testConfigParameterDefault() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        my.other.key: $brooklyn:config(\"my.param.key\")");

        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: sub-entity",
                "    item:",
                "      type: entity-with-keys");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: template",
                "  items:",
                "  - id: wrapper-entity",
                "    item:",
                "      services:",
                "      - type: entity-with-keys");

        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myDefaultVal");
        }
        
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.config:",
                    "    my.param.key: myOverrideVal");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myOverrideVal");
        }
        
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: sub-entity");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myDefaultVal");
        }
        
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: sub-entity",
                    "  brooklyn.config:",
                    "    my.param.key: myOverrideVal");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myOverrideVal");
        }
        
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: wrapper-entity");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myDefaultVal");
        }
        
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: wrapper-entity",
                    "  brooklyn.config:",
                    "    my.param.key: myOverrideVal");
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myOverrideVal");
        }
    }
    
    @Test
    public void testSubTypeUsesDefaultsFromSuper() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        my.other.key: $brooklyn:config(\"my.param.key\")");

        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: sub-entity",
                "    item:",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        my.sub.key: $brooklyn:config(\"my.param.key\")");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: sub-entity");
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.sub.key")), "myDefaultVal");
    }

    @Test
    public void testChildUsesDefaultsFromParent() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: template",
                "  items:",
                "  - id: template-with-top-level-params",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: test.parameter",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultParamVal",
                "      services:",
                "      - type: "+TestEntity.class.getName(),
                "        brooklyn.config:",
                "          " + TestEntity.ATTRIBUTE_AND_CONF_STRING.getName() + ": $brooklyn:config(\"test.parameter\")"
        );
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: template-with-top-level-params");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.sensors().get(TestEntity.ATTRIBUTE_AND_CONF_STRING), "myDefaultParamVal");
    }

    @Test
    public void testChildSoftwareProcessUsesDefaultsFromParent() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: template",
                "  items:",
                "  - id: template-with-top-level-params",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: test.parameter",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultParamVal",
                "      services:",
                "      - type: "+VanillaSoftwareProcess.class.getName(),
                "        sshMonitoring.enabled: false",
                "        " + BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION.getName() + ": true",
                "        shell.env:",
                "          TEST: $brooklyn:config(\"test.parameter\")",
                "        launch.command: |",
                "          true",
                "        checkRunning.command: |",
                "          true"
        );
        String yaml = Joiner.on("\n").join(
                "location:",
                "  localhost:",
                "    " + SshMachineLocation.SSH_TOOL_CLASS.getName() + ": " + RecordingSshTool.class.getName(),
                "services:",
                "- type: template-with-top-level-params");
        
        createStartWaitAndLogApplication(yaml);
        
        Map<?, ?> env = RecordingSshTool.getLastExecCmd().env;
        assertEquals(env.get("TEST"), "myDefaultParamVal", "env="+env);
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-328
    @Test
    public void testConfigParameterOverridingJavaConfig() throws Exception {
        String confName = TestEntity.CONF_OBJECT.getName();
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: "+confName,
                "        type: java.lang.Object",
                "        default: myDefaultObj",
                "      brooklyn.config:",
                "        my.other.obj: $brooklyn:config(\""+confName+"\")");

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.obj")), "myDefaultObj");
    }

    @Test
    public void testConfigParameterPassedFromOuterConfigParameter() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        key2: $brooklyn:config(\"my.param.key\")");

        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: wrapper-entity",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        default: myDefaultValInOuter",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        key3: $brooklyn:config(\"my.param.key\")",
                "        key3.from.root: $brooklyn:scopeRoot().config(\"my.param.key\")");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: wrapper-entity",
                "  brooklyn.config:",
                "    key4: $brooklyn:config(\"my.param.key\")",
                "    key4.from.root: $brooklyn:scopeRoot().config(\"my.param.key\")");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.param.key")), "myDefaultValInOuter");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key2")), "myDefaultValInOuter");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key3")), "myDefaultValInOuter");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key3.from.root")), "myDefaultValInOuter");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key4")), "myDefaultValInOuter");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key4.from.root")), "myDefaultValInOuter");
    }
    
    @Test
    public void testConfigParameterInSubInheritsDefaultFromYaml() throws Exception {
    	// TODO note that the corresponding functionality to inherit config info from a *java* config key is not supported
    	// see notes in BasicParameterSpec
    	
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        description: description one",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        key2: $brooklyn:config(\"my.param.key\")");

        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: wrapper-entity",
                "    item:",
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        description: description two",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        key3: $brooklyn:config(\"my.param.key\")",
                "        key3.from.root: $brooklyn:scopeRoot().config(\"my.param.key\")");

        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: wrapper-entity",
                "  brooklyn.config:",
                "    key4: $brooklyn:config(\"my.param.key\")",
                "    key4.from.root: $brooklyn:scopeRoot().config(\"my.param.key\")");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        LOG.info("Config keys declared on "+entity+": "+entity.config().findKeysDeclared(Predicates.alwaysTrue()));
        ConfigKey<?> key = Iterables.getOnlyElement( entity.config().findKeysDeclared(ConfigPredicates.nameEqualTo("my.param.key")) );
        assertEquals(key.getDescription(), "description two");
        assertEquals(entity.config().get(key), "myDefaultVal");
        
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.param.key")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key2")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key3")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key3.from.root")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key4")), "myDefaultVal");
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("key4.from.root")), "myDefaultVal");
    }
    
    @Test
    public void testSubTypeUsesDefaultsFromSuperInConfigMerging() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+VanillaSoftwareProcess.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: string",
                "        default: myDefaultVal",
                "      brooklyn.config:",
                "        shell.env:",
                "          KEY_IN_SUPER: $brooklyn:config(\"my.param.key\")",
                "        launch.command: myLaunchCmd");

        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: sub-entity",
                "    item:",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        shell.env:",
                "          KEY_IN_SUB: myBoringVal");
        
        String yaml = Joiner.on("\n").join(
                "location:",
                "  localhost:",
                "    sshToolClass: "+RecordingSshTool.class.getName(),
                "services:",
                "- type: sub-entity");
        createStartWaitAndLogApplication(yaml);
        
        ExecCmd cmd = ExecCmdAsserts.findExecContaining(RecordingSshTool.getExecCmds(), "myLaunchCmd");
        assertEquals(cmd.env.get("KEY_IN_SUPER"), "myDefaultVal", "cmd="+cmd);
        assertEquals(cmd.env.get("KEY_IN_SUB"), "myBoringVal", "cmd="+cmd);
    }
    
    @Test
    public void testConfigParametersTypes() throws Exception {
        Map<String, Class<?>> keys = ImmutableMap.<String, Class<?>>builder()
                .put("bool", Boolean.class)
                .put("boolean", Boolean.class)
                .put("Boolean", Boolean.class)
                .put("byte", Byte.class)
                .put("Byte", Byte.class)
                .put("char", Character.class)
                .put("character", Character.class)
                .put("Character", Character.class)
                .put("short", Short.class)
                .put("Short", Short.class)
                .put("int", Integer.class)
                .put("integer", Integer.class)
                .put("Integer", Integer.class)
                .put("long", Long.class)
                .put("Long", Long.class)
                .put("float", Float.class)
                .put("Float", Float.class)
                .put("double", Double.class)
                .put("Double", Double.class)
                .put("string", String.class)
                .put("String", String.class)
                .put("duration", Duration.class)
                .put("Duration", Duration.class)
                .put("timestamp", Date.class)
                .put("Timestamp", Date.class)
                .put("port", PortRange.class)
                .put("Port", PortRange.class)
                .build();
        
        List<String> catalogYaml = MutableList.of(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:");
        for (Map.Entry<String, Class<?>> entry : keys.entrySet()) {
                catalogYaml.add("      - name: "+entry.getKey()+"_key");
                catalogYaml.add("        type: "+entry.getKey());
        }
        
        addCatalogItems(catalogYaml);
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        for (Map.Entry<String, Class<?>> entry : keys.entrySet()) {
            String keyName = entry.getKey()+"_key";
            assertEquals(entity.getEntityType().getConfigKey(keyName).getType(), entry.getValue());
        }
    }

    @Test
    public void testConfigParameterWithEntitySpecAsDefault() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: "+EntitySpec.class.getName(),
                "        default: ",
                "          $brooklyn:entitySpec:",
                "          - type: "+BasicApplication.class.getName());

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");

        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        Object defaultVal = entity.config().get(entity.getEntityType().getConfigKey("my.param.key"));
        assertTrue(defaultVal instanceof EntitySpec, "defaultVal="+defaultVal);
        assertEquals(((EntitySpec<?>)defaultVal).getType(), BasicApplication.class, "defaultVal="+defaultVal);

        Entity child = entity.addChild((EntitySpec<?>)defaultVal);
        assertTrue(child instanceof BasicApplication, "child="+child);
    }
    
    @Test
    public void testManuallyAdd() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: "+TestEntity.class.getName());

        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity1 = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        TestEntity entity2 = entity1.addChild(EntitySpec.create(TestEntity.class));
        entity2.start(Collections.<Location>emptyList());
        
        Entities.dumpInfo(app);
        
        LOG.info("E1 keys: "+entity1.getEntityType().getConfigKeys());
        LOG.info("E2 keys: "+entity2.getEntityType().getConfigKeys());
        Assert.assertEquals(entity2.getEntityType().getConfigKeys(), entity1.getEntityType().getConfigKeys());
        Assert.assertEquals(entity1.getCatalogItemId(), null);
        Assert.assertEquals(entity2.getCatalogItemId(), null);
    }
    
    @Test
    public void testManuallyAddWithParentFromCatalog() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  itemType: entity",
            "  items:",
            "  - id: test-entity",
            "    item:",
            "      type: "+TestEntity.class.getName());
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: test-entity");

        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity1 = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        TestEntity entity2 = entity1.addChild(EntitySpec.create(TestEntity.class));
        entity2.start(Collections.<Location>emptyList());
        
        Entities.dumpInfo(app);
        
        LOG.info("E1 keys: "+entity1.getEntityType().getConfigKeys());
        LOG.info("E2 keys: "+entity2.getEntityType().getConfigKeys());
        Assert.assertEquals(entity2.getEntityType().getConfigKeys(), entity1.getEntityType().getConfigKeys());
        Assert.assertEquals(entity1.getCatalogItemId(), "test-entity:0.0.0.SNAPSHOT");
        
        // TODO currently the child has item ID set from CatalogUtils.setCatalogItemIdOnAddition
        // that should set a search path instead of setting the actual item
        // (ideally we'd assert null here)
        Assert.assertEquals(entity2.getCatalogItemId(), "test-entity:0.0.0.SNAPSHOT");
    }
    

    @Test
    public void testManuallyAddInTaskOfOtherEntity() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  itemType: entity",
            "  items:",
            "  - id: test-entity",
            "    item:",
            "      type: "+TestEntity.class.getName());
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: test-entity");

        Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        TestEntity entity2 = entity1.getExecutionContext().submit(new Callable<TestEntity>() {
            public TestEntity call() {
                TestEntity entity2 = entity1.addChild(EntitySpec.create(TestEntity.class));
                entity2.start(Collections.<Location>emptyList());
                return entity2;
            } 
        }).get();
        
        Entities.dumpInfo(app);
        
        LOG.info("E1 keys: "+entity1.getEntityType().getConfigKeys());
        LOG.info("E2 keys: "+entity2.getEntityType().getConfigKeys());
        Assert.assertEquals(entity2.getEntityType().getConfigKeys(), entity1.getEntityType().getConfigKeys());
        Assert.assertEquals(entity1.getCatalogItemId(), "test-entity:0.0.0.SNAPSHOT");
        
        // TODO currently the child has item ID set from context in constructor of AbstractBrooklynObject;
        // that should set a search path instead of setting the actual item
        // (ideally we'd assert null here)
        Assert.assertEquals(entity2.getCatalogItemId(), "test-entity:0.0.0.SNAPSHOT");
    }
    
    @Test
    public void testPortSetAsAttributeOnSoftwareProcess() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+EmptySoftwareProcess.class.getName(),
                "      brooklyn.parameters:",
                "      - name: my.param.key",
                "        type: port",
                "        default: 1234");

        String yaml = Joiner.on("\n").join(
                "location:",
                "  localhost:",
                "    " + SshMachineLocation.SSH_TOOL_CLASS.getName() + ": " + RecordingSshTool.class.getName(),
                "services:",
                "- type: entity-with-keys");

        Entity app = createStartWaitAndLogApplication(yaml);
        EmptySoftwareProcess entity = (EmptySoftwareProcess) Iterables.getOnlyElement(app.getChildren());

        assertEquals(entity.config().get(ConfigKeys.newConfigKey(Object.class, "my.param.key")), PortRanges.fromInteger(1234));
        assertEquals(entity.sensors().get(Sensors.newSensor(Object.class, "my.param.key")), 1234);
    }

    @Test
    public void testConfigParameterConstraintRequired() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: testRequired",
                "        type: String",
                "        constraints:",
                "        - required");
        
        String yamlNoVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");

        String yamlWithVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    testRequired: myval");

        try {
            createStartWaitAndLogApplication(yamlNoVal);
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            // success
        }

        Entity app = createStartWaitAndLogApplication(yamlWithVal);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertKeyEquals(entity, "testRequired", null, String.class, null, "myval");

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "testRequired", null, String.class, null, "myval");
    }

    @Test
    public void testConfigParameterConstraintRegex() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: testRequired",
                "        type: String",
                "        constraints:",
                "        - regex: myprefix.*");
        
        String yamlNoVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");

        String yamlWrongVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    testRequired: wrongval");

        String yamlWithVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    testRequired: myprefix-myVal");

        try {
            createStartWaitAndLogApplication(yamlNoVal);
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            Asserts.expectedFailureContains(e, "matchesRegex"); // success
        }

        try {
            createStartWaitAndLogApplication(yamlWrongVal);
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            Asserts.expectedFailureContains(e, "Invalid value for", "wrongval"); // success
        }

        Entity app = createStartWaitAndLogApplication(yamlWithVal);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertKeyEquals(entity, "testRequired", null, String.class, null, "myprefix-myVal");

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "testRequired", null, String.class, null, "myprefix-myVal");
    }

    @Test
    public void testConfigParameterConstraintObject() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+TestEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: testRequired",
                "        type: String",
                "        constraints:",
                "        - $brooklyn:object:",
                "            type: " + PredicateRegexPojo.class.getName(),
                "            object.fields:",
                "              regex: myprefix.*");
                
        
        String yamlNoVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");

        String yamlWrongVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    testRequired: wrongval");

        String yamlWithVal = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    testRequired: myprefix-myVal");

        try {
            createStartWaitAndLogApplication(yamlNoVal);
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            Asserts.expectedFailureContains(e, "Error configuring", "PredicateRegexPojo(myprefix.*)"); // success
        }

        try {
            createStartWaitAndLogApplication(yamlWrongVal);
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            Asserts.expectedFailureContains(e, "Invalid value for", "wrongval"); // success
        }

        Entity app = createStartWaitAndLogApplication(yamlWithVal);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        assertKeyEquals(entity, "testRequired", null, String.class, null, "myprefix-myVal");

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        assertKeyEquals(newEntity, "testRequired", null, String.class, null, "myprefix-myVal");
    }

    public static class PredicateRegexPojo implements Predicate<Object> {
        private String regex;

        public void setRegex(final String regex) {
            this.regex = checkNotNull(regex, "regex");
        }

        @Override
        public boolean apply(Object input) {
            return (input instanceof String) && ((String)input).matches(regex);
        }
        
        @Override
        public String toString() {
            return "PredicateRegexPojo("+regex+")";
        }
    }

    protected <T> void assertKeyEquals(Entity entity, String keyName, String expectedDescription, Class<T> expectedType, T expectedDefaultVal, T expectedEntityVal) {
        ConfigKey<?> key = entity.getEntityType().getConfigKey(keyName);
        assertNotNull(key, "No key '"+keyName+"'; keys="+entity.getEntityType().getConfigKeys());

        assertEquals(key.getName(), keyName);
        assertEquals(key.getDescription(), expectedDescription);
        assertEquals(key.getType(), expectedType);
        assertEquals(key.getDefaultValue(), expectedDefaultVal);
        
        assertEquals(entity.config().get(key), expectedEntityVal);
    }
    
    @ImplementedBy(TestEntityWithUninheritedConfigImpl.class)
    public static interface TestEntityWithUninheritedConfig extends Entity {}
    public static class TestEntityWithUninheritedConfigImpl extends TestEntityWithUninheritedConfigImplParent implements TestEntityWithUninheritedConfig {
        public static final ConfigKey<String> P1 = ConfigKeys.builder(String.class).name("p1").typeInheritance(BasicConfigInheritance.NEVER_INHERITED).build();
    }
    public static class TestEntityWithUninheritedConfigImplParent extends AbstractEntity {
        public static final ConfigKey<String> P1 = ConfigKeys.builder(String.class).name("p1-proto").typeInheritance(BasicConfigInheritance.NEVER_INHERITED).build();
    }
    
    @Test
    public void testConfigDefaultIsNotInheritedWith_LocalDefaultResolvesWithAncestorValue_SetToTrue() throws Exception {

        addCatalogItems(
            "brooklyn.catalog:",
            "  itemType: entity",
            "  version: 0.1",
            "  items:",
            "  - id: entity-with-keys",
            "    item:",
            "      type: "+TestEntityWithUninheritedConfig.class.getName(),
            "      brooklyn.parameters:",
            "      - name: p2",
            "        type: string",
            "        inheritance.type: never",
            "      - name: my.param.key",
            "        type: string",
            "        inheritance.type: never",
            "        description: description one",
            "        default: myDefaultVal",
            "      brooklyn.config:",
            "        my.other.key: $brooklyn:config(\"my.param.key\")");

        addCatalogItems(
            "brooklyn.catalog:",
            "  itemType: entity",
            "  version: 0.1",
            "  items:",
            "  - id: wrapper-entity",
            "    item:",
            "      brooklyn.parameters:",
            "      - name: my.param.key",
            "        description: description two",
            "      type: entity-with-keys");

        String yaml = Joiner.on("\n").join(
            "brooklyn.parameters:",
            "- name: p0",
            "  type: string",
            "  inheritance.runtime: never",
            "  default: default-invisible-at-child",
            "brooklyn.config:",
            "  p0: invisible-at-child",
            "services:",
            "- type: wrapper-entity");
        final int NUM_CONFIG_KEYS_FROM_TEST_BLUEPRINT = 1;

        /* With "never" inheritance, test that p0, p1, and p2 aren't visible, and my.param.key has no default. */
        
        // check on spec
        
        AbstractBrooklynObjectSpec<?, ?> spec = mgmt().getCatalog().peekSpec(mgmt().getCatalog().getCatalogItem("wrapper-entity", null));
        Assert.assertEquals(spec.getParameters().size(), SpecParameterUnwrappingTest.NUM_ENTITY_DEFAULT_CONFIG_KEYS + NUM_CONFIG_KEYS_FROM_TEST_BLUEPRINT, 
            "params: "+spec.getParameters());
        
        Entity app = createStartWaitAndLogApplication(yaml);
        final Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        // check key values
        
        ConfigKey<?> key = Iterables.getOnlyElement( entity.config().findKeysDeclared(ConfigPredicates.nameEqualTo("my.param.key")) );
        assertEquals(key.getDescription(), "description two");
        assertEquals(entity.config().get(key), null);
        
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("p0")), null);
        assertEquals(app.config().get(ConfigKeys.newStringConfigKey("p0")), "invisible-at-child");

        // check declared keys

        // none of the p? items are present
        Asserts.assertSize(entity.config().findKeysDeclared(ConfigPredicates.nameMatchesRegex("p.*")), 0);
    }
    
}
