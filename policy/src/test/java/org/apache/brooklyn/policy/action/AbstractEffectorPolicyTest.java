/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.policy.action;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class AbstractEffectorPolicyTest extends BrooklynAppUnitTestSupport {

    protected static final AttributeSensor<Boolean> START = Sensors.newBooleanSensor("start");

    protected <T> void assertConfigEqualsEventually(Configurable obj, ConfigKey<T> running, T val) {
        Asserts.eventually(() -> obj.config().get(running), Predicates.equalTo(val));
    }

    protected void assertCallHistoryNeverContinually(TestEntity entity, String effector) {
        Asserts.continually(() -> entity.getCallHistory(), l -> !l.contains(effector));
    }

    protected void assertCallHistoryContainsEventually(TestEntity entity, String effector) {
        assertCallHistoryEventually(entity, effector, 1);
    }

    protected void assertCallHistoryEventually(TestEntity entity, String effector, int minSize) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                int size = getCallHistoryCount(entity, effector);
                assertTrue(size >= minSize, "size="+size);
            }});
    }
    
    protected void assertCallsStopEventually(TestEntity entity, String effector) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                int size1 = getCallHistoryCount(entity, effector);
                Asserts.succeedsContinually(ImmutableMap.of("timeout", Duration.millis(100)), new Runnable() {
                    public void run() {
                        int size2 = getCallHistoryCount(entity, effector);
                        assertEquals(size1, size2);
                    }});
            }});
    }

    protected int getCallHistoryCount(TestEntity entity, String effector) {
        List<String> callHistory = entity.getCallHistory();
        synchronized (callHistory) {
            return Iterables.size(Iterables.filter(callHistory, Predicates.equalTo("myEffector")));
        }
    }
}
