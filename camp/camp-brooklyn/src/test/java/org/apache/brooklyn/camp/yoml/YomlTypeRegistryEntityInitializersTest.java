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
package org.apache.brooklyn.camp.yoml;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.yoml.types.YomlInitializers;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class YomlTypeRegistryEntityInitializersTest extends AbstractYamlTest {

    @Test(enabled=false) // format still runs old camp parse, does not attempt yaml
    public void testStaticSensorBasic() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.initializers:",
                "    - name: the-answer",
                "      type: static-sensor",
                "      value: 42");

        checkStaticSensor(yaml);
    }

    @Test
    public void testStaticSensorSingletonMap() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.initializers:",
                "    the-answer:",
                "      type: static-sensor",
                "      value: 42");

        checkStaticSensor(yaml);
    }

    protected void checkStaticSensor(String yaml)
        throws Exception, InterruptedException, ExecutionException, TimeoutException {
        // TODO not finding/loading type for serializers
        // TODO logically how should it learn details of static-sensor serialization?
        YomlInitializers.install(mgmt());
        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        Assert.assertEquals(
            entity.getExecutionContext().submit( 
                DependentConfiguration.attributeWhenReady(entity, Sensors.newIntegerSensor("the-answer")) )
                .get( Duration.FIVE_SECONDS ), (Integer) 42);
    }

}
