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

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityType;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.os.Os;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ConfigInheritanceYamlTest extends AbstractYamlTest {
    
    // TOOD Add tests similar to testEntityTypeInheritanceOptions, for locations, policies and enrichers.
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ConfigInheritanceYamlTest.class);

    private File emptyFile;
    private File emptyFile2;
    private File emptyFile3;
    
    private ExecutorService executor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        executor = Executors.newCachedThreadPool();
        
        emptyFile = Os.newTempFile("ConfigInheritanceYamlTest", ".txt");
        emptyFile2 = Os.newTempFile("ConfigInheritanceYamlTest2", ".txt");
        emptyFile3 = Os.newTempFile("ConfigInheritanceYamlTest3", ".txt");
        
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
                "        "+emptyFile.getAbsolutePath()+": myfile",
                "      files.preinstall:",
                "        "+emptyFile.getAbsolutePath()+": myfile",
                "      templates.install:",
                "        "+emptyFile.getAbsolutePath()+": myfile",
                "      files.install:",
                "        "+emptyFile.getAbsolutePath()+": myfile",
                "      templates.runtime:",
                "        "+emptyFile.getAbsolutePath()+": myfile",
                "      files.runtime:",
                "        "+emptyFile.getAbsolutePath()+": myfile",
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
                "  itemType: location",
                "  name: Localhost (stubbed-SSH)",
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
        if (emptyFile != null) emptyFile.delete();
        if (emptyFile2 != null) emptyFile2.delete();
        if (emptyFile3 != null) emptyFile3.delete();
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
                ImmutableMap.of(emptyFile.getAbsolutePath(), "myfile"),
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
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    files.preinstall:",
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    templates.install:",
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    files.install:",
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    templates.runtime:",
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    files.runtime:",
                "      "+emptyFile2.getAbsolutePath()+": myfile2",
                "    provisioning.properties:",
                "      mykey2: myval2",
                "      templateOptions:",
                "        myOptionsKey2: myOptionsVal2");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV1", "myEnv1", "ENV2", "myEnv2"),
                ImmutableMap.of(emptyFile.getAbsolutePath(), "myfile", emptyFile2.getAbsolutePath(), "myfile2"),
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
                null,
                ImmutableMap.of("mykey", "myval", "templateOptions", ImmutableMap.of("myOptionsKey", "myOptionsVal")));
    }
    
    // Fixed Sept 2016, attributeWhenReady self-reference is now resolved against the entity defining the config value.
    // Prior to this it was resolved against the caller's scope.
    @Test
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
            @Override
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
            @Override
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
                "      ENV: myEnv",
                "      ENV3: myEnv",
                "    templates.preinstall:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    files.preinstall:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    templates.install:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    files.install:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    templates.runtime:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    files.runtime:",
                "      "+emptyFile.getAbsolutePath()+": myfile",
                "      "+emptyFile3.getAbsolutePath()+": myfile3",
                "    provisioning.properties:",
                "      mykey: myval",
                "      mykey3: myval",
                "      templateOptions:",
                "        myOptionsKey: myOptionsVal", 
                "        myOptionsKey3: myOptionsVal", 
                "  brooklyn.children:",
                "  - type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      shell.env:",
                "        ENV2: myEnv2",
                "        ENV3: myEnv3",
                "      templates.preinstall:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      files.preinstall:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      templates.install:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      files.install:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      templates.runtime:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      files.runtime:",
                "        "+emptyFile2.getAbsolutePath()+": myfile2",
                "        "+emptyFile3.getAbsolutePath()+": myfile3",
                "      provisioning.properties:",
                "        mykey2: myval2",
                "        mykey3: myval3",
                "        templateOptions:",
                "          myOptionsKey2: myOptionsVal2",
                "          myOptionsKey3: myOptionsVal3");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEmptySoftwareProcessConfig(
                entity,
                ImmutableMap.of("ENV", "myEnv", "ENV2", "myEnv2", "ENV3", "myEnv3"),
                ImmutableMap.of(emptyFile.getAbsolutePath(), "myfile", emptyFile2.getAbsolutePath(), "myfile2", emptyFile3.getAbsolutePath(), "myfile3"),
                ImmutableMap.of("mykey", "myval", "mykey2", "myval2", "mykey3", "myval3", 
                    "templateOptions", ImmutableMap.of("myOptionsKey", "myOptionsVal", "myOptionsKey2", "myOptionsVal2", "myOptionsKey3", "myOptionsVal3")));
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
                "      - name: map.type-overwrite",
                "        type: java.util.Map",
                "        inheritance.type: overwrite",
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
                "        map.type-overwrite:",
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
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }

        // Test override defaults
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey2: myval2",
                    "    map.type-overwrite:",
                    "      mykey2: myval2",
                    "    map.type-never:",
                    "      mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("mykey2", "myval2"));
        }

        // Test retrieval of explicit config
        // TODO what should "never" mean here? Should we get the default value?
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-conf");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }
        
        // Test merging/overriding of explicit config
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-conf",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey2: myval2",
                    "    map.type-overwrite:",
                    "      mykey2: myval2",
                    "    map.type-never:",
                    "      mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("mykey2", "myval2"));
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
                "      - name: map.type-overwrite",
                "        type: java.util.Map",
                "        inheritance.parent: overwrite",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-never",
                "        type: java.util.Map",
                "        inheritance.parent: never",
                "        default: {myDefaultKey: myDefaultVal}",
                "      - name: map.type-not-reinherited",
                "        type: java.util.Map",
                "        inheritance.parent: not_reinherited",
                "        default: {myDefaultKey: myDefaultVal}");
        
        // An entity with same key names as "entity-with-keys", but no default values.
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys-dups-no-defaults",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: map.type-merged",
                "        type: java.util.Map",
                "        inheritance.parent: deep_merge",
                "      - name: map.type-overwrite",
                "        type: java.util.Map",
                "        inheritance.parent: overwrite",
                "      - name: map.type-never",
                "        type: java.util.Map",
                "        inheritance.parent: never",
                "      - name: map.type-not-reinherited",
                "        type: java.util.Map",
                "        inheritance.parent: not_reinherited");

        // Test retrieval of defaults
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-not-reinherited", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        }

        // Test override defaults in parent entity
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-overwrite:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "    map.type-not-reinherited:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(entity, "map.type-not-reinherited", ImmutableMap.of("mykey", "myval"));
        }

        // Test merging/overriding of explicit config
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-overwrite:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "    map.type-not-reinherited:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys",
                    "    brooklyn.config:",
                    "      map.type-merged:",
                    "        mykey2: myval2",
                    "      map.type-overwrite:",
                    "        mykey2: myval2",
                    "      map.type-never:",
                    "        mykey2: myval2",
                    "      map.type-not-reinherited:",
                    "        mykey2: myval2");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    
            assertConfigEquals(entity, "map.type-merged", ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-overwrite", ImmutableMap.of("mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-never", ImmutableMap.of("mykey2", "myval2"));
            assertConfigEquals(entity, "map.type-not-reinherited", ImmutableMap.of("mykey2", "myval2"));
        }
        
        // Test inheritance by child that does not explicitly declare those keys itself
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-overwrite:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "    map.type-not-reinherited:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: " + TestEntity.class.getName());
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            TestEntity child = (TestEntity) Iterables.getOnlyElement(entity.getChildren());

            // Not using `assertConfigEquals(child, ...)`, bacause child.getEntityType().getConfigKey() 
            // will not find these keys.
            //
            // TODO Note the different behaviour for "never" and "not-reinherited" keys for if an 
            // untyped key is used, versus the strongly typed key. We can live with that - I wouldn't
            // expect an implementation of TestEntity to try to use such keys that it doesn't know 
            // about!
            assertEquals(child.config().get(entity.getEntityType().getConfigKey("map.type-merged")), ImmutableMap.of("mykey", "myval"));
            assertEquals(child.config().get(entity.getEntityType().getConfigKey("map.type-overwrite")), ImmutableMap.of("mykey", "myval"));
            assertEquals(child.config().get(entity.getEntityType().getConfigKey("map.type-never")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertEquals(child.config().get(entity.getEntityType().getConfigKey("map.type-not-reinherited")), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            
            assertEquals(child.config().get(ConfigKeys.newConfigKey(Map.class, "map.type-merged")), ImmutableMap.of("mykey", "myval"));
            assertEquals(child.config().get(ConfigKeys.newConfigKey(Map.class, "map.type-overwrite")), ImmutableMap.of("mykey", "myval"));
            assertEquals(child.config().get(ConfigKeys.newConfigKey(Map.class, "map.type-never")), null);
            assertEquals(child.config().get(ConfigKeys.newConfigKey(Map.class, "map.type-not-reinherited")), null);
        }
        
        // Test inheritance by child with duplicated keys that have no defaults - does it get runtime-management parent's defaults?
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys-dups-no-defaults");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            TestEntity child = (TestEntity) Iterables.getOnlyElement(entity.getChildren());
    
            // Note this merges the default values from the parent and child (i.e. the child's default 
            // value does not override the parent's.
            assertConfigEquals(child, "map.type-merged", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(child, "map.type-overwrite", ImmutableMap.of("myDefaultKey", "myDefaultVal"));
            assertConfigEquals(child, "map.type-never", null);
            assertConfigEquals(child, "map.type-not-reinherited", null);
        }
        
        // Test inheritance by child with duplicated keys that have no defaults - does it get runtime-management parent's explicit vals?
        {
            String yaml = Joiner.on("\n").join(
                    "services:",
                    "- type: entity-with-keys",
                    "  brooklyn.config:",
                    "    map.type-merged:",
                    "      mykey: myval",
                    "    map.type-overwrite:",
                    "      mykey: myval",
                    "    map.type-never:",
                    "      mykey: myval",
                    "    map.type-not-reinherited:",
                    "      mykey: myval",
                    "  brooklyn.children:",
                    "  - type: entity-with-keys-dups-no-defaults");
            
            Entity app = createStartWaitAndLogApplication(yaml);
            TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
            TestEntity child = (TestEntity) Iterables.getOnlyElement(entity.getChildren());
    
            
            assertConfigEquals(child, "map.type-merged", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(child, "map.type-overwrite", ImmutableMap.of("mykey", "myval"));
            assertConfigEquals(child, "map.type-never", null);
            assertConfigEquals(child, "map.type-not-reinherited", null);
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
    
    @Test
    public void testExtendsSuperTypeConfigMixingShortOverridingShortName() throws Exception {
        // fixed Sept 2016
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

    // Fixed Sept 2016, inheritance is now computed with respect to defined keys and propagated upwards when traversing ancestors
    @Test
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
        Entity entity = Iterables.find(Entities.descendantsAndSelf(app), Predicates.instanceOf(TestEntity.class));

        assertEquals(entity.config().get(entity.getEntityType().getConfigKey("map.type-merged")), 
                ImmutableMap.<String, Object>of("mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3"));
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-377
    @Test(invocationCount=20, groups="Integration")
    public void testConfigOnParentUsesConfigKeyDeclaredOnParentManyTimes() throws Exception {
        testConfigOnParentUsesConfigKeyDeclaredOnParent();
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-377
    @Test
    public void testConfigOnParentUsesConfigKeyDeclaredOnParent() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: 1.0.0",
                "  itemType: entity",
                "  items:",
                "  - id: test-entity",
                "    item:",
                "      type: "+MyTestEntity.class.getName(), 
                "  - id: test-app",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      brooklyn.parameters:",
                "      - name: myParam",
                "        type: String",
                "        default: myVal",
                "      brooklyn.config:",
                "        test.confName: $brooklyn:scopeRoot().config(\"myParam\")",
                "      brooklyn.children:",
                "      - type: test-entity"
                );

        Entity app = createStartWaitAndLogApplication(Joiner.on("\n").join(
                "services:",
                "- type: "+BasicApplication.class.getName(),
                "  brooklyn.children:",
                "  - type: test-app"));
        
        MyTestEntity testEntity = (MyTestEntity) Iterables.find(Entities.descendantsAndSelf(app), Predicates.instanceOf(MyTestEntity.class));
        assertEquals(testEntity.config().get(MyTestEntity.CONF_NAME), "myVal");
        assertEquals(testEntity.sensors().get(MyTestEntity.MY_SENSOR), "myVal");
    }

    @ImplementedBy(MyTestEntityImpl.class)
    public static interface MyTestEntity extends TestEntity {
        public static final AttributeSensor<String> MY_SENSOR = Sensors.newStringSensor("myTestEntity.sensor");
    }
    
    public static class MyTestEntityImpl extends TestEntityImpl implements MyTestEntity {
        @Override
        public void start(Collection<? extends Location> locs) {
            String val = getConfig(CONF_NAME);
            sensors().set(MY_SENSOR, val);
            super.start(locs);
            
        }
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

    /**
     * Retrieves a config value using the named key - the key is looked up in two ways:
     * <ul>
     *   <li>Lookup the key on the entity (see {@link EntityType#getConfigKey(String)})
     *   <li>Construct a new untyped key (see {@link ConfigKeys#newConfigKey(Class, String)})
     * </ul>
     */
    private void assertConfigEquals(Entity entity, String keyName, Object expected) {
        ConfigKey<?> key1 = entity.getEntityType().getConfigKey(keyName);
        ConfigKey<?> key2 = ConfigKeys.newConfigKey(Object.class, keyName);
        assertEquals(entity.config().get(key1), expected, "key="+keyName);
        assertEquals(entity.config().get(key2), expected, "key="+keyName);
    }
}
