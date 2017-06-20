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
package org.apache.brooklyn.location.jclouds;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * A default no-op location customizer, which can be extended to override the appropriate methods.
 * <p>
 * When the class is used as an {@link EntityInitializer} it inserts itself into the entity's
 * {@link JcloudsLocationConfig#JCLOUDS_LOCATION_CUSTOMIZERS} under the
 * {@link BrooklynConfigKeys#PROVISIONING_PROPERTIES} key.
 */
public class BasicJcloudsLocationCustomizer extends BasicConfigurableObject implements JcloudsLocationCustomizer, EntityInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(BasicJcloudsLocationCustomizer.class);

    public BasicJcloudsLocationCustomizer() {
        this(ImmutableMap.of());
    }

    public BasicJcloudsLocationCustomizer(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public BasicJcloudsLocationCustomizer(final ConfigBag params) {
        for (Map.Entry<String, Object> entry : params.getAllConfig().entrySet()) {
            config().set(ConfigKeys.newConfigKey(Object.class, entry.getKey()), entry.getValue());
        }
    }

    @Override
    public void apply(EntityLocal entity) {
        ConfigKey<Object> subkey = BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS.getName());
        // newInstance handles the case that provisioning properties is null.
        ConfigBag provisioningProperties = ConfigBag.newInstance(entity.config().get(BrooklynConfigKeys.PROVISIONING_PROPERTIES));
        Collection<JcloudsLocationCustomizer> existingCustomizers = provisioningProperties.get(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS);
        List<? super JcloudsLocationCustomizer> merged;
        if (existingCustomizers == null) {
            merged = ImmutableList.<JcloudsLocationCustomizer>of(this);
        } else {
            merged = Lists.newArrayListWithCapacity(1 + existingCustomizers.size());
            merged.addAll(existingCustomizers);
            merged.add(this);
        }
        LOG.debug("{} set location customizers on {}: {}", new Object[]{this, entity, Iterables.toString(merged)});
        entity.config().set(subkey, merged);
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateBuilder templateBuilder) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, Template template) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, NodeMetadata node, ConfigBag setup) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        // no-op
    }

    @Override
    public void preRelease(JcloudsMachineLocation machine) {
        // no-op
    }

    @Override
    public void postRelease(JcloudsMachineLocation machine) {
        // no-op
    }

    @Override
    public void preReleaseOnObtainError(JcloudsLocation jcloudsLocation, @Nullable JcloudsMachineLocation machineLocation, Exception cause) {
        // no-op
    }

    @Override
    public void postReleaseOnObtainError(JcloudsLocation jcloudsLocation, @Nullable JcloudsMachineLocation machineLocation, Exception cause) {
        // no-op
    }

    /**
     * @return the calling entity
     */
    protected Entity getCallerContext(JcloudsMachineLocation machine) {
        Object context = config().get(LocationConfigKeys.CALLER_CONTEXT);
        if (context == null) {
            context = machine.config().get(LocationConfigKeys.CALLER_CONTEXT);
        }
        if (!(context instanceof Entity)) {
            throw new IllegalStateException("Invalid location context: " + context);
        }
        return (Entity) context;
    }
}
