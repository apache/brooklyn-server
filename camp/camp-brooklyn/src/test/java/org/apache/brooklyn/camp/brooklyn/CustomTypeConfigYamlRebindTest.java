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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerializationTest.SampleBean;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Map;

@Test
public class CustomTypeConfigYamlRebindTest extends AbstractYamlRebindTest {
    private static final Logger log = LoggerFactory.getLogger(CustomTypeConfigYamlRebindTest.class);
    private Entity lastDeployedEntity;

    public static class EntityWithCustomTypeConfig extends TestEntityImpl {
        public static final MapConfigKey<SampleBean> CUSTOM_TYPE_KEY = new MapConfigKey.Builder(SampleBean.class, "customTypeKey").build();
    }

    @Test
    public void testCustomTypeKeyRebind() throws Exception {
        Application app = (Application) createAndStartApplication(Strings.lines(
                "services:",
                "- type: " + EntityWithCustomTypeConfig.class.getName(),
                "  brooklyn.config:",
                "    " + EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.getName() + ":",
                "      i1: { x: hello }"
        ));
        makeChildCustomTypeAssertions(app);

        app = rebind();
        // prior to 2022-12-01 rebind would coerce the config when setting it, changing what is actually stored in subtle ways;
        // also it could manifest errors if the config was invalid
        makeChildCustomTypeAssertions(app);
    }

    private void makeChildCustomTypeAssertions(Application app) {
        Entity child = Iterables.getOnlyElement(app.getChildren());
        Map<String, SampleBean> vm = child.config().get(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY);
        Asserts.assertEquals(vm.get("i1").x, "hello");

        Object vmu = ((BrooklynObjectInternal.ConfigurationSupportInternal) child.config()).getRaw(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY).get();
        Asserts.assertInstanceOf(vmu, Map.class);
        Asserts.assertInstanceOf( ((Map)vmu).get("i1"), Map.class);
    }

    @Test
    public void testCustomTypeKeyBadlyFormedButUsingAttributeWhenReady() throws Exception {
        Application app = (Application) createAndStartApplication(Strings.lines(
                "services:",
                "- type: " + EntityWithCustomTypeConfig.class.getName(),
                "  brooklyn.config:",
                "    " + EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.getName() + ":",
                "      i1: { x: hello, xx: $brooklyn:attributeWhenReady(\"set-later\") }"
        ));

        {
            Entity child = Iterables.getOnlyElement(app.getChildren());
            // and we can manually set the map, validation is not deep
            child.config().set((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY,
                    MutableMap.of("i2", MutableMap.of("x", "hi", "xx", DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:attributeWhenReady(\"set-later\")"))));
            // but cannot set a specific subkey, even untyped
            Asserts.assertFailsWith(
                    () ->
                            child.config().set((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.subKey("i3"),
                                    MutableMap.of("x", "hi", "xx", DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:attributeWhenReady(\"set-later\")"))),
                    e -> Asserts.expectedFailureContains(e, "Cannot coerce or set", "Unrecognized field", "xx", "SampleBean"));

            // above can be deployed, and will block during startup because unready attribute reference means its type coercion isn't attempted so doesn't fail
            // but rebinding it threw an error prior to 2022-12-01 because BasicEntityRebindSupport.addConfig tries to coerce and validate;
            // now it is fine
            app = rebind();
        }

        {
            // but throws when we get, of course (once sensor is set)
            Entity child = Iterables.getOnlyElement(app.getChildren());
            child.sensors().set(Sensors.newStringSensor("set-later"), "now-set");

            Asserts.assertFailsWith(
                    () -> child.config().get(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY),
                    e -> Asserts.expectedFailureContains(e, "Cannot resolve", "Unrecognized field", "xx", "SampleBean"));
        }
    }

    @Test
    public void testCustomTypeKeyUsingAttributeWhenReadyForStringValue() throws Exception {
        Application app = (Application) createAndStartApplication(Strings.lines(
                "services:",
                "- type: " + EntityWithCustomTypeConfig.class.getName(),
                "  brooklyn.config:",
                "    " + EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.getName() + ":",
                "      i1: { x: $brooklyn:attributeWhenReady(\"set-later\") }"
        ));

        {
            Entity child = Iterables.getOnlyElement(app.getChildren());
            child.sensors().set(Sensors.newStringSensor("set-later"), "now-set");

            // allowed to set as above

            // and can get, as map or bean
            Map<String, SampleBean> map = child.config().get(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY);
            Asserts.assertEquals(map.keySet(), MutableSet.of("i1"));
            Asserts.assertEquals(map.get("i1").x, "now-set");

            Object bean = child.config().get((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.subKey("i1"));
            Asserts.assertEquals(((SampleBean) bean).x, "now-set");

            // but not permitted to set a DSL expression to a string value at top level
            Asserts.assertFailsWith(
                    () ->
                            child.config().set((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.subKey("i2"),
                                    MutableMap.of("x", DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:attributeWhenReady(\"set-later\")"))),
                    e -> Asserts.expectedFailureContains(e, "Cannot deserialize value", "String", "from Object"));

            // but is allowed at map level (is additive)
            child.config().set((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY,
                    MutableMap.of("i3", MutableMap.of("x", DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:attributeWhenReady(\"set-later\")"))));

            // and again, works for access to child (top-level fields are resolved?) -- not sure why, might not keep
            map = child.config().get(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY);
            Asserts.assertEquals(map.keySet(), MutableSet.of("i1", "i3"));
            Asserts.assertEquals(map.get("i3").x, "now-set");

            bean = child.config().get((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.subKey("i3"));
            Asserts.assertEquals(((SampleBean) bean).x, "now-set");
        }

        app = rebind();

        {
            // and values available after rebind
            Entity child = Iterables.getOnlyElement(app.getChildren());
            child.sensors().set(Sensors.newStringSensor("set-later"), "now-set");

            Map<String, SampleBean> map = child.config().get(EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY);
            Asserts.assertEquals(map.keySet(), MutableSet.of("i1", "i3"));
            Asserts.assertEquals(map.get("i1").x, "now-set");
            Asserts.assertEquals(map.get("i3").x, "now-set");

            Object bean = child.config().get((ConfigKey) EntityWithCustomTypeConfig.CUSTOM_TYPE_KEY.subKey("i1"));
            Asserts.assertEquals(((SampleBean) bean).x, "now-set");
        }
    }
}
