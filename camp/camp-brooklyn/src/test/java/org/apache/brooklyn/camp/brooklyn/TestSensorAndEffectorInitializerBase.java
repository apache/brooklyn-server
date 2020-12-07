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

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers.InitializerPatternWithConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers.InitializerPatternWithFieldsFromConfigKeys;
import org.testng.Assert;

import com.google.common.base.Preconditions;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;

public interface TestSensorAndEffectorInitializerBase extends EntityInitializer {

    public static final String EFFECTOR_SAY_HELLO = "sayHello";
    public static final String SENSOR_LAST_HELLO = "lastHello";
    public static final String SENSOR_HELLO_DEFINED = "sensorHelloDefined";
    public static final String SENSOR_HELLO_DEFINED_EMITTED = "sensorHelloDefinedEmitted";
    public static final ConfigKey<String> CONFIG_HELLO_WORD = ConfigKeys.newStringConfigKey("helloWord", "--", "Hello");

    public abstract String helloWord();

    @Override
    public default void apply(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        Effector<String> eff = Effectors.effector(String.class, EFFECTOR_SAY_HELLO).parameter(String.class, "name").impl(
            new EffectorBody<String>() {
                @Override
                public String call(ConfigBag parameters) {
                    Object name = parameters.getStringKey("name");
                    entity().sensors().set(Sensors.newStringSensor(SENSOR_LAST_HELLO), Strings.toString(name));
                    return helloWord()+" "+name;
                }
            }).build();
        ((EntityInternal)entity).getMutableEntityType().addEffector(eff);
        
        ((EntityInternal)entity).getMutableEntityType().addSensor(Sensors.newStringSensor(SENSOR_HELLO_DEFINED));
        
        AttributeSensor<String> emitted = Sensors.newStringSensor(SENSOR_HELLO_DEFINED_EMITTED);
        ((EntityInternal)entity).getMutableEntityType().addSensor(emitted);
        entity.sensors().set(emitted, "1");
    }

    public static class TestConfigurableInitializerStatic implements TestSensorAndEffectorInitializerBase {
        public String helloWord() {
            return CONFIG_HELLO_WORD.getDefaultValue();
        }
    }
    public static class TestConfigurableInitializerSimpleField implements TestSensorAndEffectorInitializerBase {
        String helloWord = CONFIG_HELLO_WORD.getDefaultValue();

        @Override
        public String helloWord() { return helloWord; }
    }

    public static class TestConfigurableInitializerOld implements TestSensorAndEffectorInitializerBase {
        final String helloWord;
        public TestConfigurableInitializerOld(Map<String,String> params) {
            helloWord = ConfigBag.newInstance(params).get(CONFIG_HELLO_WORD);
        }
        @Override
        public String helloWord() {
            return helloWord;
        }
    }

    public static class TestConfigurableInitializerConfigBag extends InitializerPatternWithConfigKeys implements TestSensorAndEffectorInitializerBase {
        public static final ConfigKey<String> HELLO_WORD = CONFIG_HELLO_WORD;
        private TestConfigurableInitializerConfigBag() {}
        public TestConfigurableInitializerConfigBag(ConfigBag params) { super(params); }

        @Override
        public String helloWord() {
            return initParam(HELLO_WORD);
        }
    }

    public static class TestConfigurableInitializerFieldsWithConfigKeys extends InitializerPatternWithFieldsFromConfigKeys implements TestSensorAndEffectorInitializerBase {
        public static final ConfigKey<String> HELLO_WORD = CONFIG_HELLO_WORD;
        String helloWord;
        {
            addInitConfigMapping(HELLO_WORD, v -> helloWord = v);
        }
        private TestConfigurableInitializerFieldsWithConfigKeys() {
            super();
        }
        public TestConfigurableInitializerFieldsWithConfigKeys(ConfigBag params) {
            super(params);
        }

        @Override
        public String helloWord() {
            initParamsFailIfAnyUnused();
            return helloWord;
        }
    }
    
}
