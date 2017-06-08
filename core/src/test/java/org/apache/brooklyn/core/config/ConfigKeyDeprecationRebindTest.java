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
package org.apache.brooklyn.core.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.AbstractApplication;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.mgmt.rebind.AbstractRebindHistoricTest;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class ConfigKeyDeprecationRebindTest extends AbstractRebindHistoricTest {

    @Test
    public void testUsingDeprecatedName() throws Exception {
        MyApp entity = app().addChild(EntitySpec.create(MyApp.class)
                .configure("oldKey1", "myval"));
        assertEquals(entity.config().get(MyApp.KEY_1), "myval");
        
        rebind();
        
        Entity newEntity = mgmt().getEntityManager().getEntity(entity.getId());
        assertEquals(newEntity.config().get(MyApp.KEY_1), "myval");
        
        // Expect the persisted state of the entity to have used the non-deprecated name.
        String allLines = getPersistanceFileContents(BrooklynObjectType.ENTITY, newEntity.getId());
        assertFalse(allLines.contains("oldKey1"), "contains 'oldKey1', allLines="+allLines);
        assertTrue(allLines.contains("key1"), "contains 'key1', allLines="+allLines);
    }
    
    /**
     * Created with:
     * <pre>
     * {@code
     * Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(MyApp.class)
     *         .configure("oldKey1", "myval"));
     * }
     * </pre>
     */
    @Test
    public void testEntityPersistedWithDeprecatedKeyName() throws Exception {
        String appId = "pps2ttgijb";
        addMemento(BrooklynObjectType.ENTITY, "config-deprecated-key", appId);
        rebind();
        
        Entity newApp = mgmt().getEntityManager().getEntity(appId);
        assertEquals(newApp.config().get(MyApp.KEY_1), "myval");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.ENTITY, appId);
        assertFalse(allLines.contains("oldKey1"), "should not contain 'oldKey1', allLines="+allLines);
        assertTrue(allLines.contains("<key1>"), "should contain '<key1>', allLines="+allLines);
    }

    /**
     * Created with:
     * <pre>
     * {@code
     * Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(MyApp.class)
     *         .configure("oldFlagKey2", "myval"));
     * }
     * </pre>
     */
    @Test
    public void testEntityPersistedWithSetFromFlagNameOnKey() throws Exception {
        String appId = "ug77ek2tkd";
        addMemento(BrooklynObjectType.ENTITY, "config-deprecated-flagNameOnKey", appId);
        rebind();
        
        Entity newApp = mgmt().getEntityManager().getEntity(appId);
        assertEquals(newApp.config().get(MyApp.KEY_2), "myval");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.ENTITY, appId);
        assertFalse(allLines.contains("oldFlagKey2"), "should not contain 'oldFlagKey2', allLines="+allLines);
        assertTrue(allLines.contains("<key2>"), "should contain '<key1>', allLines="+allLines);
    }
    
    /**
     * Created with:
     * <pre>
     * {@code
     * Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(MyApp.class)
     *         .policy(PolicySpec.create(MyPolicy.class)
     *                  .configure("field1", "myval")));
     * }
     * </pre>
     */
    @Test
    public void testPolicyPersistedWithSetFromFlagNameOnField() throws Exception {
        String appId = "vfncjpljqf";
        String policyId = "alq7mtwv0m";
        addMemento(BrooklynObjectType.ENTITY, "config-deprecated-flagNameOnField-policyOwner", appId);
        addMemento(BrooklynObjectType.POLICY, "config-deprecated-flagNameOnField-policy", policyId);
        rebind();
        
        MyApp newApp = (MyApp) mgmt().getEntityManager().getEntity(appId);
        MyPolicy newPolicy = (MyPolicy) Iterables.find(newApp.policies(), Predicates.instanceOf(MyPolicy.class));
        assertEquals(newPolicy.getField1(), "myval");
        assertEquals(newPolicy.config().get(MyPolicy.REPLACEMENT_FOR_FIELD_1), "myval");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.POLICY, policyId);
        assertFalse(allLines.contains("<field1>"), "should not contain '<field1>', allLines="+allLines);
        assertTrue(allLines.contains("<replacementForField1>"), "should contain '<replacementForField1>', allLines="+allLines);
    }
    
    /**
     * Created with:
     * <pre>
     * {@code
     * Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(MyApp.class)
     *         .enricher(EnricherSpec.create(MyEnricher.class)
     *                  .configure("oldKey1", "myval1")
     *                  .configure("field1", "myval2")));
     * }
     * </pre>
     */
    @Test
    public void testEnricherPersisted() throws Exception {
        String appId = "sb5w8w5tq0";
        String enricherId = "j8rvs5fc16";
        addMemento(BrooklynObjectType.ENTITY, "config-deprecated-enricherOwner", appId);
        addMemento(BrooklynObjectType.ENRICHER, "config-deprecated-enricher", enricherId);
        rebind();
        
        MyApp newApp = (MyApp) mgmt().getEntityManager().getEntity(appId);
        MyEnricher newEnricher = (MyEnricher) Iterables.find(newApp.enrichers(), Predicates.instanceOf(MyEnricher.class));
        assertEquals(newEnricher.config().get(MyEnricher.KEY_1), "myval1");
        assertEquals(newEnricher.getField1(), "myval2");
        assertEquals(newEnricher.config().get(MyEnricher.REPLACEMENT_FOR_FIELD_1), "myval2");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.ENRICHER, enricherId);
        assertFalse(allLines.contains("<field1>"), "should not contain '<field1>', allLines="+allLines);
        assertFalse(allLines.contains("<oldKey1>"), "should not contain '<oldKey1>', allLines="+allLines);
        assertTrue(allLines.contains("<key1>"), "should contain '<key1>', allLines="+allLines);
        assertTrue(allLines.contains("<replacementForField1>"), "should contain '<replacementForField1>', allLines="+allLines);
    }
    
    /**
     * Created with:
     * <pre>
     * {@code
     * MyApp app = mgmt().getEntityManager().createEntity(EntitySpec.create(MyApp.class));
     * MyFeed feed = app.feeds().add(new MyFeed());
     * feed.config().set(MyFeed.KEY_1, "myval1");
     * feed.config().set(ImmutableMap.of("oldKey2", "myval2"));
     * }
     * </pre>
     */
    @Test
    public void testFeedPersisted() throws Exception {
        String appId = "d8p4p8o4x7";
        String feedId = "km6gu420a0";
        addMemento(BrooklynObjectType.ENTITY, "config-deprecated-feedOwner", appId);
        addMemento(BrooklynObjectType.FEED, "config-deprecated-feed", feedId);
        rebind();
        
        MyApp newApp = (MyApp) mgmt().getEntityManager().getEntity(appId);
        MyFeed newFeed = (MyFeed) Iterables.find(newApp.feeds().getFeeds(), Predicates.instanceOf(MyFeed.class));
        assertEquals(newFeed.config().get(MyFeed.KEY_1), "myval1");
        assertEquals(newFeed.config().get(MyFeed.KEY_2), "myval2");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.FEED, feedId);
        assertFalse(allLines.contains("<oldKey1>"), "should not contain '<oldKey1>', allLines="+allLines);
        assertFalse(allLines.contains("<oldKey2>"), "should not contain '<oldKey2>', allLines="+allLines);
        assertTrue(allLines.contains("<key1>"), "should contain '<key1>', allLines="+allLines);
        assertTrue(allLines.contains("<key2>"), "should contain '<key2>', allLines="+allLines);
    }
    
    /**
     * Created with:
     * <pre>
     * {@code
     * MyLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(MyLocation.class)
     *          .configure("field1", "myval"));
     * }
     * </pre>
     */
    @Test
    public void testLocationPersistedWithSetFromFlagNameOnField() throws Exception {
        String locId = "f4kj5hxcvx";
        addMemento(BrooklynObjectType.LOCATION, "config-deprecated-flagNameOnField-location", locId);
        rebind();
        
        MyLocation newLoc = (MyLocation) mgmt().getLocationManager().getLocation(locId);
        assertEquals(newLoc.getField1(), "myval");
        assertEquals(newLoc.config().get(MyLocation.REPLACEMENT_FOR_FIELD_1), "myval");
        
        // Expect the persisted state to have been re-written with the new key value.
        switchOriginalToNewManagementContext();
        rebind();
        
        String allLines = getPersistanceFileContents(BrooklynObjectType.LOCATION, locId);
        assertFalse(allLines.contains("<field1>"), "should not contain '<field1>', allLines="+allLines);
        assertTrue(allLines.contains("<replacementForField1>"), "should contain '<replacementForField1>', allLines="+allLines);
    }
    
    protected String getPersistanceFileContents(BrooklynObjectType type, String id) throws Exception {
        File file = getPersistanceFile(type, id);
        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        return Joiner.on("\n").join(lines);
    }
    
    /**
     * Interface previously had:
     * <pre>
     * {@code
     * ConfigKey<String> KEY_1 = ConfigKeys.newStringConfigKey("oldKey1");
     * 
     * @SetFromFlag("oldFlagKey2")
     * ConfigKey<String> KEY_2 = ConfigKeys.newStringConfigKey("key2");
     * }
     * </pre>
     */
    @ImplementedBy(MyAppImpl.class)
    public interface MyApp extends Application, EntityInternal {
        ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1")
                .build();
        
        ConfigKey<String> KEY_2 = ConfigKeys.builder(String.class, "key2")
                .deprecatedNames("oldFlagKey2")
                .build();
    }
    
    public static class MyAppImpl extends AbstractApplication implements MyApp {
        @Override
        protected void initEnrichers() {
            // no-op; no standard enrichers to keep state small and simple
        }
    }
    
    /**
     * Class previously had:
     * <pre>
     * {@code
     * @SetFromFlag("field1")
     * private String field1;
     * }
     * </pre>
     */
   public static class MyLocation extends AbstractLocation {
        public static final ConfigKey<String> REPLACEMENT_FOR_FIELD_1 = ConfigKeys.builder(String.class, "replacementForField1")
                .deprecatedNames("field1")
                .build();
        
        public String getField1() {
            return config().get(REPLACEMENT_FOR_FIELD_1);
        }
    }

    /**
     * Class previously had:
     * <pre>
     * {@code
     * @SetFromFlag("field1")
     * private String field1;
     * }
     * </pre>
     */
    public static class MyPolicy extends AbstractPolicy {
        public static final ConfigKey<String> REPLACEMENT_FOR_FIELD_1 = ConfigKeys.builder(String.class, "replacementForField1")
                .deprecatedNames("field1")
                .build();
        
        
        public String getField1() {
             return config().get(REPLACEMENT_FOR_FIELD_1);
        }
    }
    
    /**
     * Class previously had:
     * <pre>
     * {@code
     * public static final ConfigKey<String> KEY_1 = ConfigKeys.newStringConfigKey("oldKey1");
     * @SetFromFlag("field1")
     * private String field1;
     * }
     * </pre>
     */
    public static class MyEnricher extends AbstractEnricher {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1")
                .build();
        
        public static final ConfigKey<String> REPLACEMENT_FOR_FIELD_1 = ConfigKeys.builder(String.class, "replacementForField1")
                .deprecatedNames("field1")
                .build();
        
        public String getField1() {
             return config().get(REPLACEMENT_FOR_FIELD_1);
        }
        
        public String getKey1() {
            return config().get(KEY_1);
       }
    }
    /**
     * Class previously had:
     * <pre>
     * {@code
     * public static final ConfigKey<String> KEY_1 = ConfigKeys.newStringConfigKey("oldKey1");
     * @SetFromFlag("oldKey2")
     * public static final ConfigKey<String> KEY_2 = ConfigKeys.newStringConfigKey("key2");
     * }
     * </pre>
     */
    public static class MyFeed extends AbstractFeed {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1")
                .build();
        
        public static final ConfigKey<String> KEY_2 = ConfigKeys.builder(String.class, "key2")
                .deprecatedNames("oldKey2")
                .build();
    }
}
