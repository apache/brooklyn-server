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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ConfigParametersYamlTest extends AbstractYamlTest {
    @SuppressWarnings("unused")
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
        ConfigKey<?> key = entity.getEntityType().getConfigKey("testConfigParametersListedInType.mykey");
        assertNotNull(key);
        assertEquals(key.getName(), "testConfigParametersListedInType.mykey");
        assertEquals(key.getDescription(), "myDescription");
        assertEquals(key.getType(), Map.class);
        assertEquals(key.getDefaultValue(), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        
        // Check get default value
        assertEquals(entity.config().get(key), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
    }
    
    /**
     * See comment in testConfigParametersAtRootListedInTemplateSingleEntity for why we have two 
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
        
        ConfigKey<?> key = app.getEntityType().getConfigKey("test.parameter");
        assertNotNull(key, "No key 'test.parameter'; keys="+app.getEntityType().getConfigKeys());
        assertEquals(key.getDescription(), "myDescription");
        assertEquals(key.getType(), String.class);
        assertEquals(key.getDefaultValue(), "myDefaultParamVal");
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
        
        ConfigKey<?> key = entity.getEntityType().getConfigKey("test.parameter");
        assertNotNull(key, "No key 'test.parameter'; keys="+entity.getEntityType().getConfigKeys());
        assertEquals(key.getDescription(), "myDescription");
        assertEquals(key.getType(), String.class);
        assertEquals(key.getDefaultValue(), "myDefaultParamVal");
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

    // TODO: fails; it presumably gets the config key defined in java, rather than the brooklyn.parameters key
    // See https://issues.apache.org/jira/browse/BROOKLYN-328
    @Test(groups={"WIP", "Broken"})
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
    
    // TODO: fails; times out getting config. Problem is that scopeRoot() resolves to entity-with-keys!
    // Presumably because it is resolved from inside the entity-with-keys?
    // https://issues.apache.org/jira/browse/BROOKLYN-329
    @Test(groups={"WIP", "Broken"})
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
                "        my.other.key: $brooklyn:config(\"my.param.key\")");

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
                "        my.param.key: $brooklyn:scopeRoot().config(\"my.param.key\")");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: wrapper-entity");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        Asserts.assertReturnsEventually(new Runnable() {
            public void run() {
                assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("my.other.key")), "myDefaultValInOuter");
            }},
            Asserts.DEFAULT_LONG_TIMEOUT);
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
}
