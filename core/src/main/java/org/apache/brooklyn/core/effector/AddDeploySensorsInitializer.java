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
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.sensor.Sensors;

import java.util.Map;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.text.Strings;

public class AddDeploySensorsInitializer implements EntityInitializer {

    public static final String DEPLOYMENT_METADATA = "deployment.metadata";

    public static class DeploymentMetadata {
        String user;
        Instant created;

        public void read(Object inputO, boolean overwrite) {
            Map input;
            if (inputO==null) return;
            if (inputO instanceof DeploymentMetadata) {
                input = MutableMap.of("user", ((DeploymentMetadata)inputO).user,
                        "created", ((DeploymentMetadata)inputO).created);
            } else if (!(inputO instanceof Map)) {
                return;
            } else {
                input = (Map)inputO;
            }

            if (overwrite || Strings.isBlank(user)) {
                String value = Strings.toString( input.get("user") );
                if (Strings.isNonBlank(value)) user = value;
            }
            if (overwrite || created==null) {
                Instant value = TypeCoercions.tryCoerce(input.get("created"), Instant.class ).orNull();
                if (value!=null) created = value;
            }
        }
    }

    @Override
    public void apply(EntityLocal entity) {
        // We want to set the metadata only on the root node of an application
        if (entity.getParent() != null) {
            return;
        }
        EntitlementContext entitlementContext = Entitlements.getEntitlementContext();
        AttributeSensor<DeploymentMetadata> sensor = Sensors.newSensor(
                DeploymentMetadata.class,
                DEPLOYMENT_METADATA,
                "A map of metadata information about this particular deployment. Contains at least who triggered it and when.");
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);

        DeploymentMetadata result = new DeploymentMetadata();

        // will convert config, then tag, and then republish

        result.read( entity.config().get(ConfigKeys.newConfigKey(Object.class, DEPLOYMENT_METADATA)), false );
        result.read(BrooklynTags.findSingleKeyMapValue(DEPLOYMENT_METADATA, Object.class, entity.tags().getTags()), false);
        result.read(ImmutableMap.of(
                "user", entitlementContext != null
                        ? entitlementContext.user()
                        : "Unknown",
                "created", Instant.now()), false);

        entity.sensors().set(sensor, result);

    }
}
