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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.internal.EntityConfigMap;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ConfigInheritanceYamlTest extends AbstractYamlTest {
    
    // TOOD Add tests similar to testEntityTypeInheritanceOptions, for locations, policies and enrichers.
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ConfigInheritanceYamlTest.class);

    private Path emptyFile;
    private Path emptyFile2;
    
    private ExecutorService executor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        executor = Executors.newCachedThreadPool();
        
        emptyFile = Files.createTempFile("ConfigInheritanceYamlTest", ".txt");
        emptyFile2 = Files.createTempFile("ConfigInheritanceYamlTest2", ".txt");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-with-conf",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV1: myEnv1",
                "      templates.preinstall:",
                "        "+emptyFile.toUri()+": myfile",
                "      files.preinstall:",
                "        "+emptyFile.toUri()+": myfile",
                "      templates.install:",
                "        "+emptyFile.toUri()+": myfile",
                "      files.install:",
                "        "+emptyFile.toUri()+": myfile",
                "      templates.runtime:",
                "        "+emptyFile.toUri()+": myfile",
                "      files.runtime:",
                "        "+emptyFile.toUri()+": myfile",
                "      provisioning.properties:",
                "        mykey: myval",
                "        templateOptions:",
                "          myOptionsKey: myOptionsVal");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-with-env",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      env:",
                "        ENV1: myEnv1");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-with-shell.env",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV1: myEnv1");

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: localhost-stub",
                "  name: Localhost (stubbed-SSH)",
                "  itemType: location",
                "  item:",
                "    type: localhost",
                "    brooklyn.config:",
                "      sshToolClass: "+RecordingSshTool.class.getName());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) executor.shutdownNow();
        if (emptyFile != null) Files.deleteIfExists(emptyFile);
        if (emptyFile2 != null) Files.deleteIfExists(emptyFile2);
    }
    
    @Test
    public void testInheritsSuperTypeConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-conf");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV1", "myEnv1"),
                ImmutableMap.of(emptyFile.toUri().toString(), "myfile"),
                ImmutableMap.of("mykey", "myval", "templateOptions", ImmutableMap.of("myOptionsKey", "myOptionsVal")));
    }
    
    @Test
    public void testExtendsSuperTypeConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-conf",
                "  brooklyn.config:",
                "    shell.env:",
                "      ENV2: myEnv2",
                "    templates.preinstall:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    files.preinstall:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    templates.install:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    files.install:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    templates.runtime:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    files.runtime:",
                "      "+emptyFile2.toUri()+": myfile2",
                "    provisioning.properties:",
                "      mykey2: myval2",
                "      templateOptions:",
                "        myOptionsKey2: myOptionsVal2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV1", "myEnv1", "ENV2", "myEnv2"),
                ImmutableMap.of(emptyFile.toUri().toString(), "myfile", emptyFile2.toUri().toString(), "myfile2"),
                ImmutableMap.of("mykey", "myval", "mykey2", "myval2", "templateOptions", 
                        ImmutableMap.of("myOptionsKey", "myOptionsVal", "myOptionsKey2", "myOptionsVal2")));
    }
    
    @Test
    public void testInheritsParentConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    shell.env:",
                "      ENV1: myEnv1",
                "    templates.preinstall:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.preinstall:",
                "      "+emptyFile.toUri()+": myfile",
                "    templates.install:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.install:",
                "      "+emptyFile.toUri()+": myfile",
                "    templates.runtime:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.runtime:",
                "      "+emptyFile.toUri()+": myfile",
                "    provisioning.properties:",
                "      mykey: myval",
                "      templateOptions:",
                "        myOptionsKey: myOptionsVal", 
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess");

        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV1", "myEnv1"),
                ImmutableMap.of(emptyFile.toUri().toString(), "myfile"),
                ImmutableMap.of("mykey", "myval", "templateOptions", ImmutableMap.of("myOptionsKey", "myOptionsVal")));
    }
    
    /**
     * TODO This hangs because the attributeWhenReady self-reference is resolved against the entity
     * looking up the config value (i.e. the child). Therefore it waits for the TestEntity to have
     * a value for that sensor, but this never happens. The way to avoid this is to explicitly set
     * the component that the attributeWhenReady should apply to (e.g. see {@link #testInheritsParentConfigTask()}.
     * 
     * Do we want to just exclude this test? Or do we want to "fix" it? Which entity should the
     * attributeWhenReady apply to?
     */
    @Test(groups={"Broken", "WIP"}, enabled=false)
    public void testInheritsParentConfigTaskWithSelfScope() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:config(\"myOtherConf\")",
                "    test.confObject: $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    myOtherConf: myOther",
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.core.test.entity.TestEntity");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        // Task that resolves quickly
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myOther");
        
        // Task that resolves slowly
        executor.submit(new Callable<Object>() {
            public Object call() {
                return app.sensors().set(Sensors.newStringSensor("myOtherSensor"), "myObject");
            }});
        assertEquals(app.config().get(TestEntity.CONF_OBJECT), "myObject");
    }
    
    @Test
    public void testInheritsParentConfigTask() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  id: app",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:config(\"myOtherConf\")",
                "    test.confObject: $brooklyn:component(\"app\").attributeWhenReady(\"myOtherSensor\")",
                "    myOtherConf: myOther",
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.core.test.entity.TestEntity");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        // Task that resolves quickly
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myOther");
        
        // Task that resolves slowly
        executor.submit(new Callable<Object>() {
            public Object call() {
                return app.sensors().set(Sensors.newStringSensor("myOtherSensor"), "myObject");
            }});
        assertEquals(entity.config().get(TestEntity.CONF_OBJECT), "myObject");
    }
    
    @Test
    public void testInheritsParentConfigOfTypeMapWithIndividualKeys() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    test.confMapThing.mykey: myval",
                "    test.confMapThing.mykey2: $brooklyn:config(\"myOtherConf\")",
                "    myOtherConf: myOther",
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.core.test.entity.TestEntity");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myval", "mykey2", "myOther"));
    }
    
    @Test
    public void testInheritsParentConfigOfTypeMapWithOneBigVal() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    test.confMapThing:",
                "      mykey: myval",
                "      mykey2: $brooklyn:config(\"myOtherConf\")",
                "    myOtherConf: myOther",
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.core.test.entity.TestEntity");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myval", "mykey2", "myOther"));
    }
    
    @Test
    public void testOverridesParentConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    shell.env:",
                "      ENV1: myEnv1",
                "    templates.preinstall:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.preinstall:",
                "      "+emptyFile.toUri()+": myfile",
                "    templates.install:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.install:",
                "      "+emptyFile.toUri()+": myfile",
                "    templates.runtime:",
                "      "+emptyFile.toUri()+": myfile",
                "    files.runtime:",
                "      "+emptyFile.toUri()+": myfile",
                "    provisioning.properties:",
                "      mykey: myval",
                "      templateOptions:",
                "        myOptionsKey: myOptionsVal", 
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV2: myEnv2",
                "      templates.preinstall:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      files.preinstall:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      templates.install:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      files.install:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      templates.runtime:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      files.runtime:",
                "        "+emptyFile2.toUri()+": myfile2",
                "      provisioning.properties:",
                "        mykey2: myval2",
                "        templateOptions:",
                "          myOptionsKey2: myOptionsVal2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV2", "myEnv2"),
                ImmutableMap.of(emptyFile2.toUri().toString(), "myfile2"),
                ImmutableMap.of("mykey2", "myval2", "templateOptions", ImmutableMap.of("myOptionsKey2", "myOptionsVal2")));
    }
    
    @Test
    public void testEntityTypeInheritanceOptions() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: map.type-merged",
                "        type: java.util.Map",
                "        inheritance.type: deep_merge",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-always",
                "        type: java.util.Map",
                "        inheritance.type: always",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-never",
                "        type: java.util.Map",
                "        inheritance.type: none",
                "        default: {myDefaultKey: myDefaultVal}",
                "  - id: entity-with-conf",
                "    item:",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        map.type-merged:",
                "          mykey: myval",
                "        map.type-always:",
                "          mykey: myval",
                "        map.type-never:",
                "          mykey: myval");
        
        // Test retrieval of defaults
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }

        // Test override defaults
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey2: myval2",
                    "    map.type-always:",
                    "      mykey2: myval2",
                    "    map.type-never:",
                    "      mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("mykey2", "myval2"));
        }

        // Test retrieval of explicit config
        // TODO what should "never" mean here? Should we get the default value?
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-conf");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey", "myval"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("mykey", "myval"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }
        
        // Test merging/overriding of explicit config
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-conf",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey2: myval2",
                    "    map.type-always:",
                    "      mykey2: myval2",
                    "    map.type-never:",
                    "      mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("mykey2", "myval2"));
        }
    }
    
    @Test
    public void testParentInheritanceOptions() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: map.type-merged",
                "        type: java.util.Map",
                "        inheritance.parent: deep_merge",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-always",
                "        type: java.util.Map",
                "        inheritance.parent: always",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-never",
                "        type: java.util.Map",
                "        inheritance.parent: none",
                "        default: {myDefaultKey: myDefaultVal}");
        
        // Test retrieval of defaults
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }

        // Test override defaults in parent entity
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-always:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey", "myval"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("mykey", "myval"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }

        // Test merging/overriding of explicit config
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-always:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys",
                    "    brooklyn.config:",
                    "      map.type-merged:",
                    "        mykey2: myval2",
                    "      map.type-always:",
                    "        mykey2: myval2",
                    "      map.type-never:",
                    "        mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-always")), ImmutableMap.of("mykey2", "myval2"));
            assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("mykey2", "myval2"));
        }
    }
    
    @Test
    public void testExtendsSuperTypeConfigSimple() throws Exception {
        ImmutableMap<String, Object> expectedEnv = ImmutableMap.<String, Object>of("ENV1", "myEnv1", "ENV2", "myEnv2");

        // super-type has shell.env; sub-type shell.env
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-shell.env",
                "  brooklyn.config:",
                "    shell.env:",
                "      ENV2: myEnv2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, expectedEnv);
    }

    @Test
    public void testExtendsSuperTypeConfigMixingLongOverridingShortNames() throws Exception {
        ImmutableMap<String, Object> expectedEnv = ImmutableMap.<String, Object>of("ENV1", "myEnv1", "ENV2", "myEnv2");

        // super-type has env; sub-type shell.env
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-env",
                "  brooklyn.config:",
                "    shell.env:",
                "      ENV2: myEnv2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, expectedEnv);
    }
        
    @Test
    public void testExtendsSuperTypeConfigMixingShortOverridingLongName() throws Exception {
        ImmutableMap<String, Object> expectedEnv = ImmutableMap.<String, Object>of("ENV1", "myEnv1", "ENV2", "myEnv2");

        // super-type has shell.env; sub-type env
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-shell.env",
                "  brooklyn.config:",
                "    env:",
                "      ENV2: myEnv2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, expectedEnv);
    }
    
    // TODO Has never worked, and probably hard to fix?! We need to figure out that "env" corresponds to the
    // config key. Maybe FlagUtils could respect SetFromFlags when returning Map<String,ConfigKey>?
    @Test(groups={"WIP", "Broken"})
    public void testExtendsSuperTypeConfigMixingShortOverridingShortName() throws Exception {
        ImmutableMap<String, Object> expectedEnv = ImmutableMap.<String, Object>of("ENV1", "myEnv1", "ENV2", "myEnv2");

        // super-type has env; sub-type env
        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-with-env",
                "  brooklyn.config:",
                "    env:",
                "      ENV2: myEnv2");

        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, expectedEnv);
    }
    
    @Test
    public void testExtendsSuperTypeMultipleLevels() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-level1",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV1: myEnv1");

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-level2",
                "  itemType: entity",
                "  item:",
                "    type: EmptySoftwareProcess-level1",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV2: myEnv2");

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-level3",
                "  itemType: entity",
                "  item:",
                "    type: EmptySoftwareProcess-level2",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV3: myEnv3");

        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: EmptySoftwareProcess-level3");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, 
                ImmutableMap.<String, Object>of("ENV1", "myEnv1", "ENV2", "myEnv2", "ENV3", "myEnv3"));
    }

    @Test
    public void testExtendsSuperTypeMultipleLevelsInheritanceOptions() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: TestEntity-level1",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: map.type-merged",
                "        type: java.util.Map",
                "        inheritance.type: deep_merge",
                "      brooklyn.config:",
                "        map.type-merged:",
                "          mykey1: myval1");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: TestEntity-level2",
                "  itemType: entity",
                "  item:",
                "    type: TestEntity-level1",
                "    brooklyn.config:",
                "      map.type-merged:",
                "        mykey2: myval2");

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: TestEntity-level3",
                "  itemType: entity",
                "  item:",
                "    type: TestEntity-level2",
                "    brooklyn.config:",
                "      map.type-merged:",
                "        mykey3: myval3");

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: TestEntity-level3");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), 
                ImmutableMap.of("mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3"));
    }

    /**
     * TODO Has always failed, and probably hard to fix?! This is due to the way 
     * {@link EntityConfigMap#setInheritedConfig(Map, org.apache.brooklyn.util.core.config.ConfigBag)} works:
     * the parent overrides the grandparent's config. So we only get mykey2+mykey3.
     */
    @Test(groups={"Broken", "WIP"})
    public void testExtendsParentMultipleLevels() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: TestEntity-with-conf",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: map.type-merged",
                "        type: java.util.Map",
                "        inheritance.parent: deep_merge");

        String yaml = Joiner.on("\n").join(
                "location: localhost-stub",
                "services:",
                "- type: "+BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    map.type-merged:",
                "      mykey1: myval1",
                "  brooklyn.children:",
                "  - type: "+BasicApplication.class.getName(),
                "    brooklyn.config:",
                "      map.type-merged:",
                "        mykey2: myval2",
                "    brooklyn.children:",
                "    - type: TestEntity-with-conf",
                "      brooklyn.config:",
                "        map.type-merged:",
                "          mykey3: myval3");

        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.find(Entities.descendants(app), Predicates.instanceOf(TestEntity.class));

        assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), 
                ImmutableMap.<String, Object>of("mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3"));
    }

    protected void assertEmptySoftwareProcessConfig(Entity entity, Map<String, ?> expectedEnv, Map<String, String> expectedFiles, Map<String, ?> expectedProvisioningProps) {
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.SHELL_ENVIRONMENT, MutableMap.<String, Object>copyOf(expectedEnv));
        
        List<MapConfigKey<String>> keys = ImmutableList.of(EmptySoftwareProcess.PRE_INSTALL_TEMPLATES, 
                EmptySoftwareProcess.PRE_INSTALL_FILES, EmptySoftwareProcess.INSTALL_TEMPLATES, 
                EmptySoftwareProcess.INSTALL_FILES, EmptySoftwareProcess.RUNTIME_TEMPLATES, 
                EmptySoftwareProcess.RUNTIME_FILES);
        for (MapConfigKey<String> key : keys) {
            EntityAsserts.assertConfigEquals(entity, key, expectedFiles);
        }
        
        EntityAsserts.assertConfigEquals(entity, EmptySoftwareProcess.PROVISIONING_PROPERTIES, MutableMap.<String, Object>copyOf(expectedProvisioningProps));
    }
}
