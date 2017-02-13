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

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

@Test
public class ObjectsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ObjectsYamlTest.class);

    private static final AtomicBoolean managementContextInjected = new AtomicBoolean(false);
    private static final List<String> configKeys = Lists.newLinkedList();

    public static class TestObject implements ManagementContextInjectable {
        private String string;
        private Integer number;
        private Object object;

        // Factory method.
        public static TestObject newTestObjectWithNoArgs() {
            return new TestObject("myDefaultFirst", 1, "myDefaultThird");
        }

        // Factory method.
        public static TestObject newTestObject(String string, Integer number, Object object) {
            return new TestObject(string + "-suffix", number + 1, object);
        }

        // No-arg constructor.
        public TestObject() { }

        // Parameterised constructor.
        public TestObject(String string, Integer number, Object object) {
            this.string = string;
            this.number = number;
            this.object = object;
        }

        public String getString() { return string; }
        public void setString(String string) { this.string = string; }

        public Integer getNumber() { return number; }
        public void setNumber(Integer number) { this.number = number; }

        public Object getObject() { return object; }
        public void setObject(Object object) { this.object = object; }

        @Override
        public void setManagementContext(ManagementContext managementContext) {
            log.info("Detected injection of {}", managementContext);
            managementContextInjected.set(true);
        }
    }

    // TODO Copy of org.apache.brooklyn.rest.security.PasswordHasher; but this module does not
    // have access to brooklyn-rest-resources (and would create a circular reference to do so)
    public static class PasswordHasher {
        public static String sha256(String salt, String password) {
            if (salt == null) salt = "";
            byte[] bytes = (salt + password).getBytes(Charsets.UTF_8);
            HashCode hash = Hashing.sha256().hashBytes(bytes);
            return hash.toString();
        }
    }

    public static class ConfigurableObject implements Configurable {
        public static final ConfigKey<Integer> INTEGER = ConfigKeys.newIntegerConfigKey("config.number");
        @SetFromFlag("object")
        public static final ConfigKey<Object> OBJECT = ConfigKeys.newConfigKey(Object.class, "config.object");

        @SetFromFlag("flag")
        private String string;

        private Integer number;
        private Object object;
        private Double value;
        BasicConfigurationSupport configSupport = new BasicConfigurationSupport();
        
        public ConfigurableObject() { }

        public String getString() { return string; }

        public Integer getNumber() { return number; }

        public Object getObject() { return object; }

        public Double getDouble() { return value; }
        public void setDouble(Double value) { this.value = value; }

        @Override
        public <T> T getConfig(ConfigKey<T> key) {
            return config().get(key);
        }
        
        @Override
        public <T> T setConfig(ConfigKey<T> key, T value) {
            return config().set(key, value);
        }
        
        @Override
        public ConfigurationSupport config() {
            return configSupport;
        }
        
        private class BasicConfigurationSupport implements ConfigurationSupport {
            private final ConfigBag bag = new ConfigBag();
            
            @Override
            public <T> T get(ConfigKey<T> key) {
                return bag.get(key);
            }

            @Override
            public <T> T get(HasConfigKey<T> key) {
                return get(key.getConfigKey());
            }

            @Override
            public <T> T set(ConfigKey<T> key, T val) {
                log.info("Detected configuration injection for {}: {}", key.getName(), val);
                configKeys.add(key.getName());
                if ("config.number".equals(key.getName())) number = TypeCoercions.coerce(val, Integer.class);
                if ("config.object".equals(key.getName())) object = val;
                T old = bag.get(key);
                bag.configure(key, val);
                return old;
            }

            @Override
            public <T> T set(HasConfigKey<T> key, T val) {
                return set(key.getConfigKey(), val);
            }

            @Override
            public <T> T set(ConfigKey<T> key, Task<T> val) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T set(HasConfigKey<T> key, Task<T> val) {
                return set(key.getConfigKey(), val);
            }

            @Override @Deprecated
            public Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter) {
            	return findKeysDeclared(filter);
            }
            
            @Override
            public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter) {
            	return MutableSet.copyOf(Iterables.filter(bag.getAllConfigAsConfigKeyMap().keySet(), filter));
            }
            
            @Override
            public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter) {
            	return findKeysDeclared(filter);
            }
            
        }
    }

    protected Entity setupAndCheckTestEntityInBasicYamlWith(String ...extras) throws Exception {
        managementContextInjected.set(false);
        configKeys.clear();
        Entity app = createAndStartApplication(loadYaml("test-entity-basic-template.yaml", extras));
        waitForApplicationTasks(app);

        Assert.assertEquals(app.getDisplayName(), "test-entity-basic-template");

        log.info("App started:");
        Entities.dumpInfo(app);

        Assert.assertTrue(app.getChildren().iterator().hasNext(), "Expected app to have child entity");
        Entity entity = app.getChildren().iterator().next();
        Assert.assertTrue(entity instanceof TestEntity, "Expected TestEntity, found " + entity.getClass());

        return entity;
    }

    @Test
    public void testSingleEntity() throws Exception {
        setupAndCheckTestEntityInBasicYamlWith();
    }

    @Test
    public void testBrooklynObject() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
            "        object.fields:",
            "          number: 7",
            "          object:",
            "            $brooklyn:object:",
            "              type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo",
            "          string: \"frog\"");

        Object testObject = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertTrue(testObject instanceof TestObject, "Expected a TestObject: "+testObject);
        Assert.assertTrue(managementContextInjected.get());
        Assert.assertEquals(((TestObject) testObject).getNumber(), Integer.valueOf(7));
        Assert.assertEquals(((TestObject) testObject).getString(), "frog");

        Object testObjectObject = ((TestObject) testObject).getObject();
        Assert.assertTrue(testObjectObject instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+testObjectObject);
    }

    @Test
    public void testBrooklynConfigurableObject() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$ConfigurableObject",
            "        object.fields:",
            "          double: 1.4",
            "        brooklyn.config:",
            "          flag: frog",
            "          config.number: 7",
            "          object:",
            "            $brooklyn:object:",
            "              type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo");

        Object testObject = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertTrue(testObject instanceof ConfigurableObject, "Expected a ConfigurableObject: "+testObject);
        Assert.assertEquals(((ConfigurableObject) testObject).getDouble(), Double.valueOf(1.4));
        Assert.assertEquals(((ConfigurableObject) testObject).getString(), "frog");
        Assert.assertEquals(((ConfigurableObject) testObject).getNumber(), Integer.valueOf(7));

        Object testObjectObject = ((ConfigurableObject) testObject).getObject();
        Assert.assertTrue(testObjectObject instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+testObjectObject);

        Assert.assertTrue(configKeys.contains(ConfigurableObject.INTEGER.getName()), "Expected INTEGER key: "+configKeys);
        Assert.assertTrue(configKeys.contains(ConfigurableObject.OBJECT.getName()), "Expected OBJECT key: "+configKeys);
    }

    @Test
    public void testBrooklynObjectPrefix() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confListPlain:",
            "    - $brooklyn:object:",
            "        objectType: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo",
            "    - $brooklyn:object:",
            "        object_type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo",
            "    - $brooklyn:object:",
            "        type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo");

        List<?> testList = testEntity.getConfig(TestEntity.CONF_LIST_PLAIN);

        Assert.assertEquals(testList.size(), 3);
        for (Object entry : testList) {
            Assert.assertTrue(entry instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+entry);
        }
    }

    @Test
    public void testBrooklynObjectWithFunction() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
            "        object.fields:",
            "          number: 7",
            "          object:",
            "            $brooklyn:object:",
            "              type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo",
            "          string:",
            "            $brooklyn:formatString(\"%s\", \"frog\")");

        Object testObject = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertTrue(testObject instanceof TestObject, "Expected a TestObject: "+testObject);
        Assert.assertTrue(managementContextInjected.get());
        Assert.assertEquals(((TestObject) testObject).getNumber(), Integer.valueOf(7));
        Assert.assertEquals(((TestObject) testObject).getString(), "frog");

        Object testObjectObject = ((TestObject) testObject).getObject();
        Assert.assertTrue(testObjectObject instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+testObjectObject);
    }

    @Test
    public void testBrooklynObjectWithParameterisedConstructor() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
            "        constructor.args:",
            "        - frog",
            "        - 7",
            "        - $brooklyn:object:",
            "            type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo"
        );

        Object testObject = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertTrue(testObject instanceof TestObject, "Expected a TestObject: "+testObject);
        Assert.assertTrue(managementContextInjected.get());
        Assert.assertEquals(((TestObject) testObject).getNumber(), Integer.valueOf(7));
        Assert.assertEquals(((TestObject) testObject).getString(), "frog");

        Object testObjectObject = ((TestObject) testObject).getObject();
        Assert.assertTrue(testObjectObject instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+testObjectObject);
    }

    @Test
    public void testBrooklynObjectWithFactoryMethod() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
            "        factoryMethod.name: newTestObject",
            "        factoryMethod.args:",
            "        - frog",
            "        - 7",
            "        - $brooklyn:object:",
            "            type: org.apache.brooklyn.camp.brooklyn.SimpleTestPojo"
        );

        Object testObject = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertTrue(testObject instanceof TestObject, "Expected a TestObject: "+testObject);
        Assert.assertTrue(managementContextInjected.get());
        Assert.assertEquals(((TestObject) testObject).getNumber(), Integer.valueOf(8));
        Assert.assertEquals(((TestObject) testObject).getString(), "frog-suffix");

        Object testObjectObject = ((TestObject) testObject).getObject();
        Assert.assertTrue(testObjectObject instanceof SimpleTestPojo, "Expected a SimpleTestPojo: "+testObjectObject);
    }

    @Test
    public void testBrooklynObjectWithFactoryMethodNoArgs() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    test.confObject:",
            "      $brooklyn:object:",
            "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
            "        factoryMethod.name: newTestObjectWithNoArgs");

        TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
        assertEquals(testObject.getString(), "myDefaultFirst");
        assertEquals(testObject.getNumber(), Integer.valueOf(1));
        assertEquals(testObject.getObject(), "myDefaultThird");
    }

    @Test
    public void testFieldsAsDeferredSuppliers() throws Exception {
        // all fields as deferred suppliers
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    mystring: myval",
                "    myint: 123",
                "    test.confObject:",
                "      $brooklyn:object:",
                "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
                "        object.fields:",
                "          number: $brooklyn:config(\"myint\")",
                "          string: $brooklyn:config(\"mystring\")");
    
            TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
            Assert.assertEquals(testObject.getNumber(), Integer.valueOf(123));
            Assert.assertEquals(testObject.getString(), "myval");
        }
        
        // Only first field as deferred supplier
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                    "  brooklyn.config:",
                    "    mystring: myval",
                    "    myint: 123",
                    "    test.confObject:",
                    "      $brooklyn:object:",
                    "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
                    "        object.fields:",
                    "          number: $brooklyn:config(\"myint\")",
                    "          string: myval");
    
                TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
                Assert.assertEquals(testObject.getNumber(), Integer.valueOf(123));
                Assert.assertEquals(testObject.getString(), "myval");
        }
        
        // Only second field as deferred supplier
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                    "  brooklyn.config:",
                    "    mystring: myval",
                    "    myint: 123",
                    "    test.confObject:",
                    "      $brooklyn:object:",
                    "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
                    "        object.fields:",
                    "          number: 7",
                    "          string: $brooklyn:config(\"mystring\")");
    
                TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
                Assert.assertEquals(testObject.getNumber(), Integer.valueOf(7));
                Assert.assertEquals(testObject.getString(), "myval");
        }
    }

    @Test
    public void testConfigAsDeferredSuppliers() throws Exception {
        // all fields as deferred suppliers
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    mystring: myval",
                "    myint: 123",
                "    mydouble: 1.4",
                "    test.confObject:",
                "      $brooklyn:object:",
                "        type: "+ObjectsYamlTest.class.getName()+"$ConfigurableObject",
                "        object.fields:",
                "          double: $brooklyn:config(\"mydouble\")",
                "        brooklyn.config:",
                "          flag: $brooklyn:config(\"mystring\")",
                "          config.number: $brooklyn:config(\"myint\")");
    
            ConfigurableObject testObject = (ConfigurableObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
            assertEquals(testObject.getDouble(), Double.valueOf(1.4));
            assertEquals(testObject.getString(), "myval");
            assertEquals(testObject.getNumber(), Integer.valueOf(123));
        }
        
        // Only only fields (and not config) as deferred supplier
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                    "  brooklyn.config:",
                    "    mystring: myval",
                    "    myint: 123",
                    "    mydouble: 1.4",
                    "    test.confObject:",
                    "      $brooklyn:object:",
                    "        type: "+ObjectsYamlTest.class.getName()+"$ConfigurableObject",
                    "        object.fields:",
                    "          double: $brooklyn:config(\"mydouble\")",
                    "        brooklyn.config:",
                    "          flag: myval",
                    "          config.number: 123");
        
                ConfigurableObject testObject = (ConfigurableObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
                assertEquals(testObject.getDouble(), Double.valueOf(1.4));
                assertEquals(testObject.getString(), "myval");
                assertEquals(testObject.getNumber(), Integer.valueOf(123));
        }
        
        // Only config (and not fields) as deferred supplier
        {
            Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                    "  brooklyn.config:",
                    "    mystring: myval",
                    "    myint: 123",
                    "    mydouble: 1.4",
                    "    test.confObject:",
                    "      $brooklyn:object:",
                    "        type: "+ObjectsYamlTest.class.getName()+"$ConfigurableObject",
                    "        object.fields:",
                    "          double: 1.4",
                    "        brooklyn.config:",
                    "          flag: $brooklyn:config(\"mystring\")",
                    "          config.number: $brooklyn:config(\"myint\")");
        
                ConfigurableObject testObject = (ConfigurableObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
                assertEquals(testObject.getDouble(), Double.valueOf(1.4));
                assertEquals(testObject.getString(), "myval");
                assertEquals(testObject.getNumber(), Integer.valueOf(123));
        }
    }

    @Test
    public void testConstructorArgsAsDeferredSuppliers() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    mystring: myval",
                "    myint: 123",
                "    mydouble: 1.4",
                "    test.confObject:",
                "      $brooklyn:object:",
                "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
                "        constructor.args:",
                "        - $brooklyn:config(\"mystring\")",
                "        - $brooklyn:config(\"myint\")",
                "        - myThirdParam");

        TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
        assertEquals(testObject.getNumber(), Integer.valueOf(123));
        assertEquals(testObject.getString(), "myval");
        assertEquals(testObject.getObject(), "myThirdParam");
    }
    
    @Test
    public void testFactorArgsAsDeferredSuppliers() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    mystring: myval",
                "    myint: 123",
                "    mydouble: 1.4",
                "    test.confObject:",
                "      $brooklyn:object:",
                "        type: "+ObjectsYamlTest.class.getName()+"$TestObject",
                "        factoryMethod.name: newTestObject",
                "        factoryMethod.args:",
                "        - $brooklyn:config(\"mystring\")",
                "        - $brooklyn:config(\"myint\")",
                "        - myThirdParam");

        TestObject testObject = (TestObject) testEntity.getConfig(TestEntity.CONF_OBJECT);
        assertEquals(testObject.getNumber(), Integer.valueOf(124)); // factory method adds one
        assertEquals(testObject.getString(), "myval-suffix");
        assertEquals(testObject.getObject(), "myThirdParam");
    }

    @Test
    public void testCallingPasswordHasher() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
            "  brooklyn.config:",
            "    brooklyn.password: mypassword",
            "    brooklyn.password.sha256:",
            "    brooklyn.password.salt: $brooklyn:entityId()",
            "    brooklyn.password.sha256:",
            "      $brooklyn:object:",
            "        type: "+PasswordHasher.class.getName(),
            "        factoryMethod.name: sha256",
            "        factoryMethod.args:",
            "        - $brooklyn:config(\"brooklyn.password.salt\")",
            "        - $brooklyn:config(\"brooklyn.password\")");

        String salt = (String) testEntity.config().get(ConfigKeys.newConfigKey(Object.class, "brooklyn.password.salt"));
        String sha256 = (String) testEntity.config().get(ConfigKeys.newConfigKey(Object.class, "brooklyn.password.sha256"));

        assertEquals(salt, testEntity.getId());
        assertEquals(sha256, PasswordHasher.sha256(salt, "mypassword"));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
