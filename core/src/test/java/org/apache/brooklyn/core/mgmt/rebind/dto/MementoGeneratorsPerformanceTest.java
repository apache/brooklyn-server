/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.brooklyn.core.mgmt.rebind.dto;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.policy.TestEnricher;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.core.test.qa.performance.AbstractPerformanceTest;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MementoGeneratorsPerformanceTest extends AbstractPerformanceTest {

    protected int numIterations() {
        return 100;
    }
    
    @Test(groups={"Live", "Acceptance"})
    public void testGenerateEntityMemento() throws Exception {
        int numIterations = numIterations();
        double minRatePerSec = 10 * PERFORMANCE_EXPECTATION;

        // Create an entity with lots of config/parameters, and sensors
        Map<ConfigKey<?>, String> config = Maps.newLinkedHashMap();
        List<BasicSpecParameter<?>> params = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            ConfigKey<String> key = ConfigKeys.newStringConfigKey("myparam"+i);
            params.add(new BasicSpecParameter<String>("mylabel"+i, false, key));
            config.put(key, "val"+i);
        }
        Entity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_NAME, "myname")
                .configure(config)
                .parametersAdd(params)
                .tags(ImmutableList.<Object>of("tag1", "tag2"))
                .enricher(EnricherSpec.create(TestEnricher.class))
                .policy(PolicySpec.create(TestPolicy.class)));
        
        for (int i = 0; i < 100; i++) {
            AttributeSensor<String> sensor = Sensors.newStringSensor("mysensor"+i);
            entity.sensors().set(sensor, "valsensor"+i);
        }

        final Entity deproxiedEntity = Entities.deproxy(entity);

        // Run the performance test
        measure(PerformanceTestDescriptor.create()
                .summary("mementosGenerators.newBasicMemento(entity)")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .job(new Runnable() {
                    @Override public void run() {
                        MementosGenerators.newBasicMemento(deproxiedEntity);
                    }}));
    }
}
