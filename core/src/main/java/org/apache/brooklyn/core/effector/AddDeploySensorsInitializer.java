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
package org.apache.brooklyn.core.effector;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.sensor.Sensors;

import java.util.Map;

public class AddDeploySensorsInitializer implements EntityInitializer {
    @Override
    public void apply(EntityLocal entity) {
        // We want to set the metadata only on the root node of an application
        if (entity.getParent() != null) {
            return;
        }
        EntitlementContext entitlementContext = Entitlements.getEntitlementContext();
        AttributeSensor<Map<String, Object>> sensor = Sensors.newSensor(
                new TypeToken<Map<String, Object>>() {},
                "deployment.metadata",
                "A map of metadata information about this particular deployment. Contains at least who triggered it and when.");
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);
        entity.sensors().set(sensor, ImmutableMap.of(
                "user", entitlementContext != null ? entitlementContext.user() : "Unknown",

                "created", Instant.now()
                // previously used the below instead
//                "deploy_time", System.currentTimeMillis()
        ));

    }
}
