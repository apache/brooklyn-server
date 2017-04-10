/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.test.framework.entity;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;

@ImplementedBy(TestEntityImpl.class)
public interface TestEntity extends Entity, Startable {

    AttributeSensor<Boolean> SIMPLE_EFFECTOR_INVOKED = Sensors.newBooleanSensor("simple-effector-invoked");
    AttributeSensor<Boolean> COMPLEX_EFFECTOR_INVOKED = Sensors.newBooleanSensor("complex-effector-invoked");
    AttributeSensor<String> COMPLEX_EFFECTOR_STRING = Sensors.newStringSensor("complex-effector-string");
    AttributeSensor<Boolean> COMPLEX_EFFECTOR_BOOLEAN = Sensors.newBooleanSensor("complex-effector-boolean");
    AttributeSensor<Long> COMPLEX_EFFECTOR_LONG = Sensors.newLongSensor("complex-effector-long");
    AttributeSensor<Integer> FAILING_EFFECTOR_INVOCATION_COUNT = Sensors.newIntegerSensor("failing-effector-count");

    @Effector
    void simpleEffector();

    @Effector
    TestPojo complexEffector(@EffectorParam(name = "stringValue") final String stringValue,
                             @EffectorParam(name = "booleanValue") final Boolean booleanValue,
                             @EffectorParam(name = "longValue") final Long longValue);

    @Effector
    String effectorReturnsString(@EffectorParam(name = "stringToReturn") final String stringToReturn);

    @Effector
    Integer effectorReturnsInt(@EffectorParam(name = "intToReturn") Integer intToReturn);

    @Effector
    void effectorHangs();

    @Effector
    void effectorFails() throws EffectorFailureException;
    
    class EffectorFailureException extends Exception {
        private static final long serialVersionUID = -8996475930661355402L;
        
        public EffectorFailureException(String msg) {
            super(msg);
        }
        public EffectorFailureException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
    
    class TestPojo {
        private final String stringValue;
        private final Boolean booleanValue;
        private final Long longValue;

        public TestPojo(final String stringValue, final Boolean booleanValue, final Long longValue) {
            this.stringValue = stringValue;
            this.booleanValue = booleanValue;
            this.longValue = longValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public Boolean getBooleanValue() {
            return booleanValue;
        }

        public Long getLongValue() {
            return longValue;
        }
    }
}
