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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class ConfigKeyDeprecationTest extends BrooklynAppUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ConfigKeyDeprecationTest.class);
    
    public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
            .deprecatedNames("oldKey1", "oldKey1b")
            .build();

    @Test
    public void testUsingDeprecatedName() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyBaseEntity.class)
                .configure("oldSuperKey1", "myval"));
        EntityInternal entity2 = app.addChild(EntitySpec.create(MyBaseEntity.class)
                .configure("oldSuperKey1b", "myval"));
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval");
        assertEquals(entity2.config().get(MyBaseEntity.SUPER_KEY_1), "myval");
    }

    @Test
    public void testPrefersNonDeprecatedName() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyBaseEntity.class)
                .configure("superKey1", "myval")
                .configure("oldSuperKey1", "mywrongval"));
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval");
    }
    
    @Test
    public void testPrefersFirstDeprecatedNameIfMultiple() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyBaseEntity.class)
                .configure("oldSuperKey1b", "myval2")
                .configure("oldSuperKey1", "myval1"));
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval1");
    }
    
    @Test
    public void testInheritsDeprecatedKeyFromRuntimeParent() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure("oldSuperKey1", "myval"));
        EntityInternal child = entity.addChild(EntitySpec.create(MyBaseEntity.class));
        assertEquals(child.config().get(MyBaseEntity.SUPER_KEY_1), "myval");
    }

    @Test
    public void testInheritsDeprecatedKeyFromSuperType() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MySubEntity.class)
                .configure("oldSuperKey1", "myval")
                .configure("oldSuperKey2", "myval2")
                .configure("oldSubKey2", "myval3")
                .configure("oldInterfaceKey1", "myval4"));
        assertEquals(entity.config().get(MySubEntity.SUPER_KEY_1), "myval");
        assertEquals(entity.config().get(MySubEntity.SUPER_KEY_2), "myval2");
        assertEquals(entity.config().get(MySubEntity.SUB_KEY_2), "myval3");
        assertEquals(entity.config().get(MyInterface.INTERFACE_KEY_1), "myval4");
    }

    @Test
    public void testUsingDeprecatedNameInLocation() throws Exception {
        MyLocation loc = mgmt.getLocationManager().createLocation(LocationSpec.create(MyLocation.class)
                .configure("oldKey1", "myval"));
        assertEquals(loc.config().get(MyLocation.KEY_1), "myval");
    }

    @Test
    public void testUsingDeprecatedNameInPolicy() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure("oldKey1", "myval"));
        assertEquals(policy.config().get(MyPolicy.KEY_1), "myval");
    }

    @Test
    public void testUsingDeprecatedNameInEnricher() throws Exception {
        MyEnricher enricher = app.enrichers().add(EnricherSpec.create(MyEnricher.class)
                .configure("oldKey1", "myval"));
        assertEquals(enricher.config().get(MyEnricher.KEY_1), "myval");
    }

    @Test
    public void testUsingDeprecatedNameInEntityInitializer() throws Exception {
        Entity entity = app.addChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new MyEntityInitializer(ConfigBag.newInstance(ImmutableMap.of("oldKey1", "myval")))));
        assertEquals(entity.config().get(MyEntityInitializer.KEY_1), "myval");
    }

    @Test
    public void testSetConfigUsingDeprecatedName() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyBaseEntity.class));
        
        // Set using strongly typed key
        entity.config().set(MyBaseEntity.SUPER_KEY_1, "myval");
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval");
        
        // Set using correct string
        entity.config().putAll(ImmutableMap.of("superKey1", "myval2"));
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval2");
        
        // Set using deprecated name
        entity.config().putAll(ImmutableMap.of("oldSuperKey1", "myval3"));
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval3");
        
        // Set using pseudo-generated strongly typed key with deprecated name
        entity.config().set(ConfigKeys.newConfigKey(Object.class, "oldSuperKey1"), "myval4");
        assertEquals(entity.config().get(MyBaseEntity.SUPER_KEY_1), "myval4");
    }

    @Test
    public void testSetAndGetDynamicConfigUsingDeprecatedName() throws Exception {
        // Using BasicEntity, which doesn't know about KEY_1
        EntityInternal entity = (EntityInternal) app.addChild(EntitySpec.create(BasicEntity.class));
        
        // Set using deprecated name
        entity.config().putAll(ImmutableMap.of("oldKey1", "myval3"));
        assertEquals(entity.config().get(KEY_1), "myval3");
        
        // Set using pseudo-generated strongly typed key with deprecated name
        entity.config().set(ConfigKeys.newConfigKey(Object.class, "oldKey1"), "myval4");
        assertEquals(entity.config().get(KEY_1), "myval4");
    }

    /*
    // Setting the value of a "dynamic config key" when using the deprecated key will not overwrite
    // any existing value that was set using the real config key name. I think this is because 
    // EntityDynamicType.addConfigKey() is not called when KEY_1 is first set, so it doesn't have
    // access to the deprecated names on subsequent calls to set(String, val). It therefore stores
    // in EntityConfigMap (backed by a ConfigBag) the original key-value and the deprecated key-value.
    // When we subsequently call get(key), it retrieved the original key-value.
    //
    // Contrast this with EntityDynamicType.addSensorIfAbsent().
    //
     * However, it's (probably) not straight forward to just add and call a addConfigKeyIfAbsent. This
     * is because config().set() is used for dynamic config declared in yaml, which the entity doesn't
     * understand but that will be inherited by runtime children.
     */
    @Test(groups="Broken")
    public void testSetAndGetDynamicConfigUsingDeprecatedNameOverwritesExistingValue() throws Exception {
        // Using BasicEntity, which doesn't know about KEY_1
        EntityInternal entity = (EntityInternal) app.addChild(EntitySpec.create(BasicEntity.class));
        
        // Set using strongly typed key
        entity.config().set(KEY_1, "myval");
        assertEquals(entity.config().get(KEY_1), "myval");
        
        // Set using correct string
        entity.config().putAll(ImmutableMap.of("key1", "myval2"));
        assertEquals(entity.config().get(KEY_1), "myval2");
        
        // Set using deprecated name
        entity.config().putAll(ImmutableMap.of("oldKey1", "myval3"));
        assertEquals(entity.config().get(KEY_1), "myval3");
        
        // Set using pseudo-generated strongly typed key with deprecated name
        entity.config().set(ConfigKeys.newConfigKey(Object.class, "oldKey1"), "myval4");
        assertEquals(entity.config().get(KEY_1), "myval4");
    }

    @Test
    public void testLogsIfDeprecatedNameUsed() throws Exception {
        // FIXME Which logger?
        String loggerName = ConfigUtilsInternal.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.WARN;
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessages(
                "Using deprecated config value on MyBaseEntity",
                "should use 'superKey1', but used 'oldSuperKey1'");

        try (LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter)) {
            testUsingDeprecatedName();
            watcher.assertHasEvent();
        }
    }


    @Test
    public void testLogsWarningIfNonDeprecatedAndDeprecatedNamesUsed() throws Exception {
        String loggerName = ConfigUtilsInternal.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.WARN;
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessages(
                "Ignoring deprecated config value(s) on MyBaseEntity",
                "because contains value for 'superKey1', other deprecated name(s) present were: [oldSuperKey1]");
        
        try (LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter)) {
            testPrefersNonDeprecatedName();
            watcher.assertHasEvent();
        }
    }

    @Test
    public void testLogsWarningIfMultipleDeprecatedNamesUsed() throws Exception {
        String loggerName = ConfigUtilsInternal.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.WARN;
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessages(
                "Using deprecated config value on MyBaseEntity",
                "should use 'superKey1', but used 'oldSuperKey1b' and ignored values present for other deprecated name(s) [oldSuperKey1b]");

        try (LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter)) {
            testPrefersFirstDeprecatedNameIfMultiple();
            watcher.assertHasEvent();
        }
    }
    
    @ImplementedBy(MyBaseEntityImpl.class)
    public interface MyBaseEntity extends EntityInternal {
        ConfigKey<String> SUPER_KEY_1 = ConfigKeys.builder(String.class, "superKey1")
                .deprecatedNames("oldSuperKey1", "oldSuperKey1b")
                .build();
        ConfigKey<String> SUPER_KEY_2 = ConfigKeys.builder(String.class, "superKey2")
                .deprecatedNames("oldSuperKey2")
                .build();
    }
    
    public static class MyBaseEntityImpl extends AbstractEntity implements MyBaseEntity {
    }

    @ImplementedBy(MySubEntityImpl.class)
    public interface MySubEntity extends MyBaseEntity, MyInterface {
        ConfigKey<String> SUPER_KEY_1 = ConfigKeys.newConfigKeyWithDefault(MyBaseEntity.SUPER_KEY_1, "overridden superKey1 default");
        ConfigKey<String> SUB_KEY_2 = ConfigKeys.builder(String.class, "subKey2")
                .deprecatedNames("oldSubKey2")
                .build();
    }
    
    public static class MySubEntityImpl extends MyBaseEntityImpl implements MySubEntity {
    }

    public interface MyInterface {
        ConfigKey<String> INTERFACE_KEY_1 = ConfigKeys.builder(String.class, "interfaceKey1")
                .deprecatedNames("oldInterfaceKey1")
                .build();
    }

    public static class MyLocation extends AbstractLocation {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1", "oldKey1b")
                .build();
    }
    
    public static class MyPolicy extends AbstractPolicy {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1", "oldKey1b")
                .build();
    }
    
    public static class MyEnricher extends AbstractEnricher {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1", "oldKey1b")
                .build();
    }
    
    public static class MyEntityInitializer implements EntityInitializer {
        public static final ConfigKey<String> KEY_1 = ConfigKeys.builder(String.class, "key1")
                .deprecatedNames("oldKey1", "oldKey1b")
                .build();
        
        private String key1;

        public MyEntityInitializer(ConfigBag params) {
            this.key1 = params.get(KEY_1);
        }

        @Override
        public void apply(EntityLocal entity) {
            entity.config().set(KEY_1, key1);
        }
    }
}
