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
package org.apache.brooklyn.util.core.text;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import freemarker.core.InvalidReferenceException;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.FixedLocaleTest;
import org.apache.brooklyn.util.collections.MutableList;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TemplateProcessorTest extends BrooklynAppUnitTestSupport {
    private FixedLocaleTest localeFix = new FixedLocaleTest();

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        localeFix.setUp();
    }

    @Override
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        super.tearDown();
        localeFix.tearDown();
    }

    @Test
    public void testAdditionalArgs() {
        String templateContents = "${mykey}";
        String result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.of("mykey", "myval"));
        assertEquals(result, "myval");
    }
    
    @Test
    public void testEntityConfig() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_NAME, "myval"));
        String templateContents = "${config['"+TestEntity.CONF_NAME.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }
    
    @Test
    public void testEntityConfigNumber() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_OBJECT, 123456));
        String templateContents = "${config['"+TestEntity.CONF_OBJECT.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        // 2022-11 changed so ?c is the default
        assertEquals(result, "123456");
    }
    
    @Test
    public void testEntityConfigNumberComputerExplicit() {
        // ?c is needed to avoid commas (i always forget this!)
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_OBJECT, 123456));
        String templateContents = "${config['"+TestEntity.CONF_OBJECT.getName()+"']?c}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "123456");
    }


    @Test
    public void testEntityConfigNumberLocaleExplicit() {
        // ?c is needed to avoid commas (i always forget this!)
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_OBJECT, 123456));
        String templateContents = "${config['"+TestEntity.CONF_OBJECT.getName()+"']?string.number}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "123,456");
    }
    
    @Test
    public void testEntityAttribute() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        entity.sensors().set(Attributes.HOSTNAME, "myval");
        String templateContents = "${attribute['"+Attributes.HOSTNAME.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }

    @Test
    public void testEntityDisplayName() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                        .displayName("My Test Entity"));
        String templateContents = "${displayName}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "My Test Entity");
    }

    @Test
    public void testEntityApplicationAccess() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        // test getter
        String templateContents = "${application.displayName}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, app.getDisplayName());

        // test config (proves our model has intercepted)
        app.config().set(TestEntity.CONF_NAME, "sample config app");
        templateContents = "${application.config['"+TestEntity.CONF_NAME.getName()+"']}";
        result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, app.config().get(TestEntity.CONF_NAME));
    }
    
    @Test
    public void testConditionalComparingAttributes() {
        AttributeSensor<String> sensor1 = Sensors.newStringSensor("sensor1");
        AttributeSensor<String> sensor2 = Sensors.newStringSensor("sensor2");
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        entity.sensors().set(sensor1, "myval1");
        entity.sensors().set(sensor2, "myval1");
        String templateContents = Joiner.on("\n").join(
                "[#ftl]",
                "[#if attribute['sensor1'] == attribute['sensor2']]",
                "true",
                "[#else]",
                "false",
                "[/#if]");
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result.trim(), "true");
        
        entity.sensors().set(sensor2, "myval2");
        String result2 = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result2.trim(), "false");
    }
    
    @Test
    public void testGetSysProp() {
        System.setProperty("testGetSysProp", "myval");
        
        String templateContents = "${javaSysProps['testGetSysProp']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }
    
    @Test
    public void testEntityGetterMethod() {
        String templateContents = "${entity.id}";
        String result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, app.getId());
    }
    
    @Test
    public void testLocationGetterMethod() {
        LocalhostMachineProvisioningLocation location = app.newLocalhostProvisioningLocation();
        String templateContents = "${location.id}";
        String result = TemplateProcessor.processTemplateContents(templateContents, location, ImmutableMap.<String,Object>of());
        assertEquals(result, location.getId());
    }

    @Test
    public void testEntityChildrenAndMembers() {
        TestEntity child = app.createAndManageChild(EntitySpec.create(TestEntity.class).configure(BrooklynConfigKeys.PLAN_ID, "my_child"));

        assertEquals(TemplateProcessor.processTemplateContents("${entity.children[0].id}", app, null), child.getId());
        assertEquals(TemplateProcessor.processTemplateContents("${entity.children[\"my_child\"].id}", app, null), child.getId());

        Asserts.assertFailsWith(() -> TemplateProcessor.processTemplateContents("${entity.children[\"no_such_child\"].id}", app, null),
            error -> Asserts.expectedFailureContains(error, "children[\"no_such_child\"] ", InvalidReferenceException.class.getSimpleName()));

        assertEquals(TemplateProcessor.processTemplateContents("test", "${entity.children}", TemplateProcessor.EntityAndMapTemplateModel.forEntity(app, null), true, false),
                MutableList.of(child));
        assertEquals(TemplateProcessor.processTemplateContents("test", "${entity.children[0]}", TemplateProcessor.EntityAndMapTemplateModel.forEntity(app, null), true, false),
                child);
        assertEquals(TemplateProcessor.processTemplateContents("test", "${entity.children[\"my_child\"]}", TemplateProcessor.EntityAndMapTemplateModel.forEntity(app, null), true, false),
                child);

        Group group = app.createAndManageChild(EntitySpec.create(BasicGroup.class));
        group.addMember(child);

        assertEquals(TemplateProcessor.processTemplateContents("${entity.members[0].id}", (EntityInternal) group, null), child.getId());
        assertEquals(TemplateProcessor.processTemplateContents("${entity.members[\"my_child\"].id}", (EntityInternal) group, null), child.getId());
    }

    // Test takes 2.5s.
    @Test(groups = "Integration")
    public void testLocationConfig() {
        LocalhostMachineProvisioningLocation location = app.newLocalhostProvisioningLocation(ImmutableMap.of("mykey", "myval"));
        String templateContents = "${config['mykey']}";//"+TestEntity.CONF_NAME.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, location, ImmutableMap.<String, Object>of());
        assertEquals(result, "myval");
    }
    
    @Test
    public void testManagementContextConfig() {
        mgmt.getBrooklynProperties().put("globalmykey", "myval");
        String templateContents = "${mgmt.globalmykey}";
        String result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }
    
    @Test
    public void testManagementContextDefaultValue() {
        String templateContents = "${(missing)!\"defval\"}";
        Object result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, "defval");
    }
    
    @Test
    public void testManagementContextDefaultValueInDotMissingValue() {
        String templateContents = "${(mgmt.missing.more_missing)!\"defval\"}";
        Object result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, "defval");
    }
    
    @Test
    public void testManagementContextConfigWithDot() {
        mgmt.getBrooklynProperties().put("global.mykey", "myval");
        String templateContents = "${mgmt['global.mykey']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }
    
    @Test
    public void testManagementContextErrors() {
        try {
            // NB: dot has special meaning so this should fail; must be accessed using bracket notation as above
            mgmt.getBrooklynProperties().put("global.mykey", "myval");
            String templateContents = "${mgmt.global.mykey}";
            TemplateProcessor.processTemplateContents(templateContents, app, ImmutableMap.<String,Object>of());
            Assert.fail("Should not have found value with intermediate dot");
        } catch (Exception e) {
            Assert.assertTrue(e.toString().contains("global"), "Should have mentioned missing key 'global' in error");
        }
    }
    
    @Test
    public void testApplyTemplatedConfigWithAttributeWhenReady() {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");

        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_NAME, DependentConfiguration.attributeWhenReady(app, TestApplication.MY_ATTRIBUTE)));
        
        String templateContents = "${config['"+TestEntity.CONF_NAME.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, entity, ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }

    @Test
    public void testApplyTemplatedConfigWithAtributeWhenReadyInSpec() {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class).configure(DynamicCluster.INITIAL_SIZE, 0)
            .location(LocationSpec.create(LocalhostMachineProvisioningLocation.LocalhostMachine.class)));
        cluster.config().set(DynamicCluster.MEMBER_SPEC,
                EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME,
                        DependentConfiguration.attributeWhenReady(cluster, TestEntity.NAME)));
        cluster.sensors().set(TestEntity.NAME, "myval");
        cluster.resize(1);
        String templateContents = "${config['"+TestEntity.CONF_NAME.getName()+"']}";
        String result = TemplateProcessor.processTemplateContents(templateContents, (EntityInternal)Iterables.getOnlyElement(cluster.getChildren()), ImmutableMap.<String,Object>of());
        assertEquals(result, "myval");
    }

    @Test
    public void testDotSeparatedKey() {
        String templateContents = "${a.b}";
        String result = TemplateProcessor.processTemplateContents(templateContents, (ManagementContextInternal)null, 
            ImmutableMap.<String,Object>of("a.b", "myval"));
        assertEquals(result, "myval");
    }
    
    @Test
    public void testDotSeparatedKeyCollisionFailure() {
        String templateContents = "${aaa.bbb}";
        try {
            TemplateProcessor.processTemplateContents(templateContents, (ManagementContextInternal)null, 
                ImmutableMap.<String,Object>of("aaa.bbb", "myval", "aaa", "blocker"));
            Assert.fail("Should not have found value with intermediate dot where prefix is overridden");
        } catch (Exception e) {
            Assert.assertTrue(e.toString().contains("aaa"), "Should have mentioned missing key 'aaa' in error");
        }
    }

    @Test
    public void testIterateHash() {
        String templateContents = "<#list h as k,v>${k} = ${v}</#list>";
        String result = TemplateProcessor.processTemplateContents(templateContents, (ManagementContextInternal) null,
                ImmutableMap.of("h", ImmutableMap.of("war", "peace")));
        assertEquals(result, "war = peace");
    }

}
