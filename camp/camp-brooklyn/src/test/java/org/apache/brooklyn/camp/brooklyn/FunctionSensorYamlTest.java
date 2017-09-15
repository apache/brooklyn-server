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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.function.FunctionSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class FunctionSensorYamlTest extends AbstractYamlRebindTest {
    private static final Logger log = LoggerFactory.getLogger(FunctionSensorYamlTest.class);

    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static String TARGET_TYPE = "java.lang.String";

    public static class MyCallable implements Callable<Object> {
        public static AtomicReference<Object> val = new AtomicReference<>();

        @Override public Object call() throws Exception {
            return val.get();
        }
    }

    @Test
    public void testFunctionSensor() throws Exception {
        MyCallable.val.set("first");
        
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "  brooklyn.initializers:",
            "  - type: "+FunctionSensor.class.getName(),
            "    brooklyn.config:",
            "      "+FunctionSensor.SENSOR_PERIOD.getName()+": 100ms",
            "      "+FunctionSensor.SENSOR_NAME.getName()+": " + SENSOR_STRING.getName(),
            "      "+FunctionSensor.SENSOR_TYPE.getName()+": " + TARGET_TYPE,
            "      "+FunctionSensor.FUNCTION.getName()+":",
            "        $brooklyn:object:",
            "          type: "+MyCallable.class.getName());
        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "first");
        
        MyCallable.val.set("second");
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "second");
        
        // Rebind, and confirm that it resumes polling
        Application newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());

        MyCallable.val.set("third");
        EntityAsserts.assertAttributeEqualsEventually(newEntity, SENSOR_STRING, "third");
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
