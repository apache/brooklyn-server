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
package org.apache.brooklyn.cm.salt;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.cm.salt.impl.SaltEntityImpl;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import java.util.List;

@ImplementedBy(SaltEntityImpl.class)
@Catalog(name="SaltEntity", description="Software managed by Salt CM")
public interface SaltEntity extends SoftwareProcess, SaltConfig {

    @SetFromFlag("version")
    ConfigKey<String> SUGGESTED_VERSION = ConfigKeys.newConfigKeyWithDefault(
        BrooklynConfigKeys.SUGGESTED_VERSION, "stable");

    AttributeSensor<List<String>> STATES = Sensors.newSensor(new TypeToken<List<String>>() {}, "salt.states",
        "Salt Highstate states");

    MethodEffector<String> SALT_CALL = new MethodEffector<>(SaltEntity.class, "saltCall");

    @Effector(description = "Invoke a Salt command")
    String saltCall(
        @EffectorParam(name = "spec", description = "Name and optional arguments of a Salt command") String spec);

}

